use std::{env::args, fs::OpenOptions, os::linux::fs::MetadataExt as _, thread::scope};

use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use dashmap::DashSet;
use nohash::BuildNoHashHasher;

macro_rules! die {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1)
    }};
}
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
}

type WorkerRx = Receiver<DirEntry>;
type WorkerTx = Sender<DirEntry>;
// blocking syscall: ioctl, should be run on multiple threads
struct Worker<'a> {
    rx: WorkerRx,
    stat: CompsizeStat,
    sv2_arg: Sv2Args,
    extent_map: &'a ExtentMap,
}
impl<'a> Worker<'a> {
    fn new(recv: WorkerRx, extent_map: &'a ExtentMap) -> Self {
        Self {
            rx: recv,
            stat: CompsizeStat::default(),
            sv2_arg: Sv2Args::new(),
            extent_map,
        }
    }

    fn run(mut self) -> CompsizeStat {
        while let Ok(entry) = self.rx.recv() {
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
                    for (key, comp, estat) in iter.map(|item| item.parse().unwrap()) {
                        merge_stat(&self.extent_map, key, comp, estat, &mut self.stat);
                    }
                }
                Err(e) => {
                    // todo!() // search_v2 not supported
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
    scope(|ex| {
        let args: Vec<_> = args().skip(1).collect();
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
                    do_file(entry, &ftx);
                }
            }
        });
        let handles: Vec<_> = (0..4)
            .map(|_| {
                let worker = Worker::new(frx.clone(), &extent_map);
                ex.spawn(|| worker.run())
            })
            .collect();
        let final_stat = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .reduce(|mut a, b| {
                a.merge(b);
                a
            })
            .unwrap();
        println!("{:#?}", final_stat);
    });
}
