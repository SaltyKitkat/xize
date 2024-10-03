use std::{
    env::args,
    fmt::Display,
    fs::OpenOptions,
    os::linux::fs::MetadataExt as _,
    process,
    sync::atomic::{AtomicBool, Ordering},
    thread::scope,
};

use crossbeam::channel::{unbounded, Receiver, Sender};
use dashmap::DashSet;
use nohash::BuildNoHashHasher;

mod btrfs;
use btrfs::Sv2Args;
use walkdir::{DirEntry, WalkDir};

type ExtentMap = DashSet<u64, BuildNoHashHasher<u64>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
struct ExtentStat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}
impl ExtentStat {
    fn merge(&mut self, rhs: Self) {
        self.disk += rhs.disk;
        self.uncomp += rhs.uncomp;
        self.refd += rhs.refd;
    }
    fn is_empty(&self) -> bool {
        self.disk == 0 && self.uncomp == 0 && self.refd == 0
    }
}

#[derive(Debug, Default)]
struct CompsizeStat {
    nfile: u64,
    ninline: u64,
    nref: u64,
    nextent: u64,
    prealloc: ExtentStat,
    stat: [ExtentStat; 4],
}

impl CompsizeStat {
    fn merge(&mut self, rhs: Self) {
        self.nfile += rhs.nfile;
        self.ninline += rhs.ninline;
        self.nref += rhs.nref;
        self.nextent += rhs.nextent;
        self.prealloc.merge(rhs.prealloc);
        for (l, r) in self.stat.iter_mut().zip(rhs.stat) {
            l.merge(r);
        }
    }
    fn display(&self, ty: DisplayType, scale: Scale) -> CompsizeStatDisplay<'_> {
        CompsizeStatDisplay(self, ty, scale)
    }
}
enum DisplayType {
    Human,
    K,
    M,
    G,
    T,
}
enum Scale {
    Metric,
    Binary,
}
struct CompsizeStatDisplay<'a>(&'a CompsizeStat, DisplayType, Scale);
impl<'a> Display for CompsizeStatDisplay<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Processed {} files, {} regular extents ({} refs), {} inline.",
            self.0.nfile, self.0.nextent, self.0.nref, self.0.ninline
        )?;
        todo!()
    }
}

// Processed 3356969 files, 653492 regular extents (2242077 refs), 2018321 inline.
// Type       Perc     Disk Usage   Uncompressed Referenced
// TOTAL       78%     100146085502 127182733170 481020538738
// none       100%     88797796415  88797796415  364255758399
// zstd        29%     11348289087  38384936755  116764780339

type WorkerRx = Receiver<DirEntry>;
type WorkerTx = Sender<DirEntry>;
// blocking syscall: ioctl, should be run on multiple threads
struct Worker<'map, 'sig> {
    rx: WorkerRx,
    stat: CompsizeStat,
    sv2_arg: Sv2Args,
    extent_map: &'map ExtentMap,
    quit_sig: &'sig AtomicBool,
}
impl<'map, 'sig> Worker<'map, 'sig> {
    fn new(recv: WorkerRx, extent_map: &'map ExtentMap, quit_sig: &'sig AtomicBool) -> Self {
        Self {
            rx: recv,
            stat: CompsizeStat::default(),
            sv2_arg: Sv2Args::new(),
            extent_map,
            quit_sig,
        }
    }

    fn run(mut self) -> CompsizeStat {
        while let Ok(entry) = self.rx.recv() {
            if self.quit_sig.load(Ordering::Acquire) {
                break;
            }
            let file = OpenOptions::new()
                .read(true)
                .write(false)
                // .custom_flags(O_NOFOLLOW | O_NOCTTY | O_NONBLOCK)
                .open(entry.path())
                .unwrap();
            let ino = entry.metadata().unwrap().st_ino();
            match self.sv2_arg.search_file(file, ino) {
                Ok(iter) => {
                    self.stat.nfile += 1;
                    for (key, comp, estat) in iter.filter_map(|item| item.parse().unwrap()) {
                        merge_stat(self.extent_map, key, comp, estat, &mut self.stat);
                    }
                }
                Err(e) => {
                    self.quit_sig.store(true, Ordering::Release);
                    if e.raw_os_error() == 25 {
                        eprintln!(
                            "{}: Not btrfs (or SEARCH_V2 unsupported)",
                            entry.path().display()
                        );
                    } else {
                        eprintln!("{}: SEARCH_V2: {}", entry.path().display(), e);
                    }
                    break;
                }
            }
        }
        self.stat
    }
}
fn merge_stat(
    extent_map: &ExtentMap,
    key: btrfs::ExtentKey,
    comp: btrfs::Compression,
    stat: ExtentStat,
    ret: &mut CompsizeStat,
) {
    match key.r#type() {
        btrfs::ExtentType::Inline => {
            ret.ninline += 1;
            ret.stat[comp.as_usize()].disk += stat.disk;
            ret.stat[comp.as_usize()].uncomp += stat.uncomp;
            ret.stat[comp.as_usize()].refd += stat.refd;
        }
        btrfs::ExtentType::Regular => {
            ret.nref += 1;
            if extent_map.insert(key.key()) {
                ret.nextent += 1;
                ret.stat[comp.as_usize()].disk += stat.disk;
                ret.stat[comp.as_usize()].uncomp += stat.uncomp;
            }
            ret.stat[comp.as_usize()].refd += stat.refd;
        }
        btrfs::ExtentType::Prealloc => {
            ret.nref += 1;
            if extent_map.insert(key.key()) {
                ret.nextent += 1;
                ret.prealloc.disk += stat.disk;
                ret.prealloc.uncomp += stat.uncomp;
            }
            ret.prealloc.refd += stat.refd;
        }
    }
}
fn do_file(entry: DirEntry, workers: &WorkerTx) {
    workers.send(entry).unwrap();
}

fn main() {
    let (ftx, frx) = unbounded();
    let extent_map = DashSet::with_hasher(BuildNoHashHasher::default());
    let quit_sig = AtomicBool::new(false);
    let final_stat = scope(|ex| {
        let args: Vec<_> = args().skip(1).collect();
        {
            let quit_sig = &quit_sig;
            ex.spawn(move || {
                for arg in args {
                    for entry in WalkDir::new(arg)
                        .follow_links(false)
                        .into_iter()
                        .filter_map(|e| {
                            let e = e.ok()?;
                            if e.metadata().unwrap().is_file() {
                                Some(e)
                            } else {
                                None
                            }
                        })
                    {
                        if quit_sig.load(Ordering::Acquire) {
                            return;
                        }
                        do_file(entry, &ftx);
                    }
                }
            });
        }
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let worker = Worker::new(frx.clone(), &extent_map, &quit_sig);
                ex.spawn(|| worker.run())
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .reduce(|mut a, b| {
                a.merge(b);
                a
            })
            .unwrap()
    });
    if quit_sig.load(Ordering::Acquire) {
        process::exit(1);
    }
    println!("{:#?}", final_stat);
}
