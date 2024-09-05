use std::{env::args, os::linux::fs::MetadataExt as _, path::PathBuf, sync::Arc};

use btrfs::Sv2Args;
use easy_parallel::Parallel;
use libc::{O_NOCTTY, O_NOFOLLOW, O_NONBLOCK};
use nohash::IntSet;
use smol::{
    block_on,
    channel::{bounded, unbounded, Receiver, Sender},
    fs::{read_dir, unix::OpenOptionsExt as _, DirEntry, OpenOptions},
    stream::StreamExt as _,
    Executor, Task,
};

macro_rules! die {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1)
    }};
}
mod btrfs {

    use std::{fmt::Debug, iter::FusedIterator};

    type File = smol::fs::File;

    use rustix::{
        io::Errno,
        ioctl::{ioctl, ReadWriteOpcode, Updater},
    };

    use crate::ExtentStat;

    pub const BTRFS_IOCTL_MAGIC: u8 = 0x94;
    pub const BTRFS_EXTENT_DATA_KEY: u32 = 108;
    pub const BTRFS_FILE_EXTENT_INLINE: u8 = 0;
    pub const BTRFS_FILE_EXTENT_REG: u8 = 1;
    pub const BTRFS_FILE_EXTENT_PREALLOC: u8 = 2;

    // pub const struct_btrfs_ioctl_search_header = extern struct {
    //     transid: __u64 = @import("std").mem.zeroes(__u64),
    //     objectid: __u64 = @import("std").mem.zeroes(__u64),
    //     offset: __u64 = @import("std").mem.zeroes(__u64),
    //     type: __u32 = @import("std").mem.zeroes(__u32),
    //     len: __u32 = @import("std").mem.zeroes(__u32),
    // };

    // le on disk
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct IoctlSearchHeader {
        transid: u64,
        objectid: u64,
        offset: u64,
        r#type: u32,
        len: u32,
    }
    impl IoctlSearchHeader {
        unsafe fn from_le_raw(buf: &[u8]) -> Self {
            let raw = &*(buf.as_ptr() as *const IoctlSearchHeader);
            Self {
                transid: u64::from_le(raw.transid),
                objectid: u64::from_le(raw.objectid),
                offset: u64::from_le(raw.offset),
                r#type: u32::from_le(raw.r#type),
                len: u32::from_le(raw.len),
            }
        }
    }

    // const file_extent_item = packed struct {
    //     generation: u64,
    //     ram_bytes: u64,
    //     compression: u8,
    //     encryption: u8,
    //     other_encoding: u16,
    //     type: u8,
    //     disk_bytenr: u64,
    //     disk_num_bytes: u64,
    //     offset: u64,
    //     num_bytes: u64,
    // };
    // le on disk
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(packed)]
    pub struct FileExtentItem {
        pub generation: u64,
        pub ram_bytes: u64,
        pub compression: u8,
        pub encryption: u8,
        pub other_encoding: u16,
        pub r#type: u8,
        // following u64 * 4 for regular extent, or inline data for inline extent
        pub disk_bytenr: u64,
        pub disk_num_bytes: u64,
        pub offset: u64,
        pub num_bytes: u64,
    }
    const EXTENT_INLINE_HEADER_SIZE: usize = 21;
    impl FileExtentItem {
        unsafe fn from_le_raw(buf: &[u8]) -> Self {
            let raw = &*(buf.as_ptr() as *const FileExtentItem);
            Self {
                generation: u64::from_le(raw.generation),
                ram_bytes: u64::from_le(raw.ram_bytes),
                compression: u8::from_le(raw.compression),
                encryption: u8::from_le(raw.encryption),
                other_encoding: u16::from_le(raw.other_encoding),
                r#type: u8::from_le(raw.r#type),
                disk_bytenr: u64::from_le(raw.disk_bytenr),
                disk_num_bytes: u64::from_le(raw.disk_num_bytes),
                offset: u64::from_le(raw.offset),
                num_bytes: u64::from_le(raw.num_bytes),
            }
        }
    }

    #[repr(packed)]
    pub struct IoctlSearchItem {
        pub(self) header: IoctlSearchHeader,
        pub(self) item: FileExtentItem,
    }
    #[repr(u8)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Compression {
        None = 0,
        Zlib,
        Lzo,
        Zstd,
    }
    impl Compression {
        pub fn as_usize(self) -> usize {
            self as usize
        }
        pub fn from_usize(n: u8) -> Self {
            match n {
                0 => Self::None,
                1 => Self::Zlib,
                2 => Self::Lzo,
                3 => Self::Zstd,
                _ => panic!("Invalid compression type: {}", n),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub enum ExtentType {
        Inline,
        Regular,
        Prealloc,
    }

    impl ExtentType {
        pub fn from_u8(n: u8) -> Self {
            match n {
                BTRFS_FILE_EXTENT_INLINE => Self::Inline,
                BTRFS_FILE_EXTENT_REG => Self::Regular,
                BTRFS_FILE_EXTENT_PREALLOC => Self::Prealloc,
                _ => panic!("Invalid extent type: {}", n),
            }
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct ExtentKey {
        r#type: ExtentType,
        key: u64, // invalid if r#type == Inline
    }
    impl ExtentKey {
        pub fn new(r#type: ExtentType, key: u64) -> Self {
            Self { r#type, key }
        }

        pub fn r#type(&self) -> ExtentType {
            self.r#type
        }

        pub fn key(&self) -> u64 {
            self.key
        }
    }

    impl IoctlSearchItem {
        unsafe fn from_le_raw(buf: &[u8]) -> Self {
            let header = IoctlSearchHeader::from_le_raw(&buf[..size_of::<IoctlSearchHeader>()]);
            let item = FileExtentItem::from_le_raw(&buf[size_of::<IoctlSearchHeader>()..]);
            Self { header, item }
        }
        pub fn parse(&self) -> Result<(ExtentKey, Compression, ExtentStat), String> {
            let hlen = self.header.len;
            let ram_bytes = self.item.ram_bytes;
            let comp_type = Compression::from_usize(self.item.compression);
            let extent_type = ExtentType::from_u8(self.item.r#type);
            if extent_type == ExtentType::Inline {
                const EXTENT_INLINE_HEADER_SIZE: usize = 21;
                let disk_num_bytes = hlen as u64 - EXTENT_INLINE_HEADER_SIZE as u64;
                // build result
                return Ok((
                    ExtentKey::new(extent_type, 0),
                    comp_type,
                    ExtentStat {
                        disk: disk_num_bytes,
                        uncomp: ram_bytes,
                        refd: ram_bytes,
                    },
                ));
            }
            if hlen != size_of::<FileExtentItem>() as u32 {
                let errmsg = format!("Regular extent's header not 53 bytes ({}) long?!?", hlen,);
                return Err(errmsg);
            }
            let disk_bytenr = self.item.disk_bytenr;
            // is hole
            if disk_bytenr == 0 {
                return Ok((
                    ExtentKey::new(extent_type, 0),
                    comp_type,
                    ExtentStat {
                        disk: 0,
                        uncomp: 0,
                        refd: 0,
                    },
                ));
            }
            // check 4k alignment
            if disk_bytenr & 0xfff != 0 {
                let errmsg = format!("Extent not 4k aligned at ({:#x})", disk_bytenr);
                return Err(errmsg);
            }
            let disk_bytenr = disk_bytenr >> 12;
            let disk_num_bytes = self.item.disk_num_bytes;
            let num_bytes = self.item.num_bytes;
            Ok((
                ExtentKey::new(extent_type, disk_bytenr),
                comp_type,
                ExtentStat {
                    disk: disk_num_bytes,
                    uncomp: ram_bytes,
                    refd: num_bytes,
                },
            ))
        }
    }

    // le on disk
    // #[repr(packed)]
    // pub struct FileExtentItem {
    //     header: FileExtentItemHeader,
    //     pub disk_bytenr: u64,
    //     pub disk_num_bytes: u64,
    //     pub offset: u64,
    //     pub num_bytes: u64,
    // }

    // sv2_args->key.tree_id = 0;
    // sv2_args->key.max_objectid = st_ino;
    // sv2_args->key.min_objectid = st_ino;
    // sv2_args->key.min_offset = 0;
    // sv2_args->key.max_offset = -1;
    // sv2_args->key.min_transid = 0;
    // sv2_args->key.max_transid = -1;
    // // Only search for EXTENT_DATA_KEY
    // sv2_args->key.min_type = BTRFS_EXTENT_DATA_KEY;
    // sv2_args->key.max_type = BTRFS_EXTENT_DATA_KEY;
    // sv2_args->key.nr_items = -1;
    // sv2_args->buf_size = sizeof(sv2_args->buf);
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct IoctlSearchKey {
        tree_id: u64,
        min_objectid: u64,
        max_objectid: u64,
        min_offset: u64,
        max_offset: u64,
        min_transid: u64,
        max_transid: u64,
        min_type: u32,
        max_type: u32,
        nr_items: u32,
        unused: u32,
        unused1: u64,
        unused2: u64,
        unused3: u64,
        unused4: u64,
    }

    impl IoctlSearchKey {
        fn new(st_ino: u64) -> Self {
            Self {
                tree_id: 0,
                min_objectid: st_ino,
                max_objectid: st_ino,
                min_offset: 0,
                max_offset: u64::MAX,
                min_transid: 0,
                max_transid: u64::MAX,
                min_type: BTRFS_EXTENT_DATA_KEY,
                max_type: BTRFS_EXTENT_DATA_KEY,
                nr_items: u32::MAX,
                unused: 0,
                unused1: 0,
                unused2: 0,
                unused3: 0,
                unused4: 0,
            }
        }
    }

    // should be reused for different files
    #[derive(Debug, PartialEq, Eq, Hash)]
    #[repr(C)]
    pub struct Sv2Args {
        key: IoctlSearchKey,
        buf_size: u64,
        buf: [u8; 65536],
    }

    impl Sv2Args {
        pub fn new() -> Self {
            Self {
                key: IoctlSearchKey::new(0),
                buf_size: 65536,
                buf: [0; 65536],
            }
        }

        fn set_key(&mut self, ino: u64) {
            self.key = IoctlSearchKey::new(ino);
        }

        pub fn search_file(&mut self, fd: File, ino: u64) -> rustix::io::Result<Sv2ItemIter> {
            self.set_key(ino);
            Sv2ItemIter::new(self, fd)
        }
    }
    #[derive(Debug)]
    pub struct Sv2ItemIter<'arg> {
        sv2_arg: &'arg mut Sv2Args,
        fd: File,
        pos: usize,
        nrest_item: u32,
        last: bool,
    }
    impl<'arg> Iterator for Sv2ItemIter<'arg> {
        type Item = IoctlSearchItem;

        fn next(&mut self) -> Option<Self::Item> {
            if self.need_ioctl() {
                self.call_ioctl().unwrap();
            }
            if self.finish() {
                return None;
            }
            let ret = unsafe { IoctlSearchItem::from_le_raw(&self.sv2_arg.buf[self.pos..]) };
            self.pos += size_of::<IoctlSearchHeader>() + ret.header.len as usize;
            self.nrest_item -= 1;
            if self.nrest_item == 0 {
                self.sv2_arg.key.min_offset = ret.header.offset + 1;
            }
            Some(ret)
        }
    }
    impl FusedIterator for Sv2ItemIter<'_> {}
    impl<'arg> Sv2ItemIter<'arg> {
        fn call_ioctl(&mut self) -> Result<(), Errno> {
            unsafe {
                let ctl = Updater::<'_, ReadWriteOpcode<BTRFS_IOCTL_MAGIC, 17, Sv2Args>, _>::new(
                    self.sv2_arg,
                );
                ioctl(&self.fd, ctl)?;
            }
            self.nrest_item = self.sv2_arg.key.nr_items;
            self.last = self.nrest_item <= 512;
            self.pos = 0;
            Ok(())
        }
        fn need_ioctl(&self) -> bool {
            self.nrest_item == 0 && !self.last
        }
        fn finish(&self) -> bool {
            self.nrest_item == 0 && self.last
        }
        pub fn new(sv2_arg: &'arg mut Sv2Args, fd: File) -> Result<Self, Errno> {
            sv2_arg.key.nr_items = u32::MAX;
            sv2_arg.key.min_offset = 0;
            // other fields not reset, maybe error?
            let mut ret = Self {
                sv2_arg,
                fd,
                pos: 0,
                nrest_item: 0,
                last: false,
            };
            ret.call_ioctl()?;
            Ok(ret)
        }
    }
}

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

struct ExtentMap {
    map: IntSet<u64>,
    rx: Receiver<(u64, Sender<bool>)>,
}

impl ExtentMap {
    fn new(recv: Receiver<(u64, Sender<bool>)>) -> Self {
        Self {
            map: IntSet::default(),
            rx: recv,
        }
    }

    async fn run(mut self) -> usize {
        while let Ok((key, tx)) = self.rx.recv().await {
            tx.send(self.map.insert(key)).await.unwrap();
        }
        self.map.len()
    }
}

// blocking syscall: ioctl, should be run on multiple threads
struct Worker {
    rx: Receiver<(DirEntry, Sender<CompsizeStat>)>,
    sv2_arg: Sv2Args,
    extent_map: Sender<(u64, Sender<bool>)>,
}
impl Worker {
    fn new(
        recv: Receiver<(DirEntry, Sender<CompsizeStat>)>,
        extent_map: Sender<(u64, Sender<bool>)>,
    ) -> Self {
        Self {
            rx: recv,
            sv2_arg: Sv2Args::new(),
            extent_map,
        }
    }

    async fn run(mut self) {
        while let Ok((entry, sender)) = self.rx.recv().await {
            let mut ret = CompsizeStat::default();
            let file = OpenOptions::new()
                .read(true)
                .write(false)
                .custom_flags(O_NOFOLLOW | O_NOCTTY | O_NONBLOCK)
                .open(entry.path())
                .await
                .unwrap();
            let ino = entry.metadata().await.unwrap().st_ino();

            match self.sv2_arg.search_file(file, ino) {
                Ok(iter) => {
                    for (key, comp, stat) in iter.map(|item| item.parse().unwrap()) {
                        match key.r#type() {
                            btrfs::ExtentType::Inline => {
                                ret.ninline += 1;
                                ret.stat[comp.as_usize()].disk += stat.disk;
                                ret.stat[comp.as_usize()].uncomp += stat.uncomp;
                                ret.stat[comp.as_usize()].refd += stat.refd;
                            }
                            btrfs::ExtentType::Regular => {
                                ret.nref += 1;
                                let (tx, rx) = bounded(1);
                                self.extent_map.send((key.key(), tx)).await.unwrap();
                                if rx.recv().await.unwrap() {
                                    ret.stat[comp.as_usize()].disk += stat.disk;
                                    ret.stat[comp.as_usize()].uncomp += stat.uncomp;
                                }
                                ret.stat[comp.as_usize()].refd += stat.refd;
                            }
                            btrfs::ExtentType::Prealloc => {
                                ret.nref += 1;
                                let (tx, rx) = bounded(1);
                                self.extent_map.send((key.key(), tx)).await.unwrap();
                                if rx.recv().await.unwrap() {
                                    ret.prealloc.disk += stat.disk;
                                    ret.prealloc.uncomp += stat.uncomp;
                                }
                                ret.prealloc.refd += stat.refd;
                            }
                        }
                    }
                }
                Err(e) => {
                    todo!()
                }
            }
            ret.nfile += 1;
            sender.send(ret).await.ok();
        }
    }
}

fn do_direntry(
    ex: Arc<Executor<'_>>,
    dir: PathBuf,
    extent_map: Sender<(u64, Sender<bool>)>,
    workers: Sender<(DirEntry, Sender<CompsizeStat>)>,
) -> Task<CompsizeStat> {
    ex.clone().spawn(async move {
        let mut dir = read_dir(dir).await.unwrap();
        let mut handles = vec![];
        while let Some(entry) = dir.next().await {
            let entry = entry.unwrap();
            let file_type = entry.file_type().await.unwrap();
            if file_type.is_dir() {
                handles.push(do_direntry(
                    ex.clone(),
                    entry.path(),
                    extent_map.clone(),
                    workers.clone(),
                ));
            } else if file_type.is_file() {
                let workers = workers.clone();
                handles.push(do_file(&ex, entry, workers));
            }
        }
        let mut ret = CompsizeStat::default();
        for handle in handles {
            ret.merge(handle.await);
        }
        ret
    })
}

fn do_file(
    ex: &Executor,
    entry: DirEntry,
    workers: Sender<(DirEntry, Sender<CompsizeStat>)>,
) -> Task<CompsizeStat> {
    ex.spawn(async move {
        let (tx, rx) = bounded(1);
        workers.send((entry, tx)).await.unwrap();
        rx.recv().await.unwrap()
    })
}

fn main() {
    let (etx, erx) = bounded(16);
    let (ftx, frx) = bounded(16);
    let extent_map = ExtentMap::new(erx);
    let ex = Arc::new(Executor::new());
    let (signal, shutdown) = unbounded::<()>();
    let mut tasks = vec![];
    let etask = ex.spawn(async {
        let ret = extent_map.run().await;
        drop(signal);
        ret
    });
    ex.spawn_many(
        (0..12).map(|_| {
            let worker = Worker::new(frx.clone(), etx.clone());
            worker.run()
        }),
        &mut tasks,
    );
    let args: Vec<_> = args().skip(1).collect();
    let mut handles = vec![];
    for arg in args {
        let path = PathBuf::from(arg);
        let handle = if path.is_dir() {
            do_direntry(ex.clone(), path, etx.clone(), ftx.clone())
        } else {
            // let ino = path.metadata().unwrap().st_ino();
            // let file = block_on(
            //     OpenOptions::new()
            //         .read(true)
            //         .write(false)
            //         .custom_flags(O_NOFOLLOW | O_NOCTTY | O_NONBLOCK)
            //         .open(path),
            // )
            // .unwrap();
            // do_file(&ex, file, ino, ftx.clone())
            todo!("single file")
        };
        handles.push(handle);
    }
    drop(ftx);
    drop(etx);
    Parallel::new()
        // Run executor threads.
        .each(0..4, |_| {
            block_on(ex.run(shutdown.recv())).ok();
        })
        // Run the main future on the current thread.
        .run();
    let final_stat = block_on(async {
        let mut ret = CompsizeStat::default();
        for handle in handles {
            ret.merge(handle.await)
        }
        ret.nextent = etask.await as _;
        ret
    });
    println!("{:#?}", final_stat);
}
