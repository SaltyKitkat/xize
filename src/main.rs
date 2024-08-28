use std::{
    collections::HashSet,
    env::args,
    fs::OpenOptions,
    os::linux::fs::MetadataExt,
    path::{Display, PathBuf},
};

use btrfs::{Compression, ExtentKey};
use libc::{O_NOCTTY, O_NOFOLLOW, O_NONBLOCK};
use rustix::{
    fd::AsFd,
    fs::{Dev, OpenOptionsExt},
    io::Errno,
};

macro_rules! die {
    ($($arg:tt)*) => {{
        eprintln!($($arg)*);
        std::process::exit(1)
    }};
}
mod btrfs {

    use std::{fmt::Debug, iter::FusedIterator};

    use rustix::{
        fd::AsFd,
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
    #[derive(Debug)]
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
        #[inline]
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
    #[derive(Debug)]
    #[repr(C)]
    pub struct Sv2Args {
        key: IoctlSearchKey,
        buf_size: u64,
        buf: [u8; 65536],
    }

    impl Sv2Args {
        #[inline]
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

        pub fn search_file<F: AsFd>(
            &mut self,
            fd: F,
            ino: u64,
        ) -> rustix::io::Result<Sv2ItemIter<F>> {
            self.set_key(ino);
            Sv2ItemIter::new(self, fd)
        }
    }
    #[derive(Debug)]
    pub struct Sv2ItemIter<'arg, F> {
        sv2_arg: &'arg mut Sv2Args,
        fd: F,
        pos: usize,
        offset: u64,
        nrest_item: u32,
        finish: bool,
    }
    impl<'arg, F: AsFd> Iterator for Sv2ItemIter<'arg, F> {
        type Item = IoctlSearchItem;

        fn next(&mut self) -> Option<Self::Item> {
            if self.nrest_item == 0 {
                if self.finish {
                    return None;
                } else {
                    self.call_ioctl().unwrap();
                }
            }
            let ret = unsafe { IoctlSearchItem::from_le_raw(&self.sv2_arg.buf[self.pos..]) };
            self.pos += size_of::<IoctlSearchHeader>() + ret.header.len as usize;
            self.nrest_item -= 1;
            if self.nrest_item == 0 {
                self.offset = ret.header.offset;
            }
            Some(ret)
        }
    }
    // impl<F: AsFd> FusedIterator for Sv2ItemIter<'_, F> {}
    impl<'arg, F: AsFd> Sv2ItemIter<'arg, F> {
        fn call_ioctl(&mut self) -> Result<(), Errno> {
            self.sv2_arg.key.min_offset = self.offset;
            unsafe {
                let ctl = Updater::<'_, ReadWriteOpcode<BTRFS_IOCTL_MAGIC, 17, Sv2Args>, _>::new(
                    self.sv2_arg,
                );
                ioctl(&self.fd, ctl)?;
            }
            self.nrest_item = self.sv2_arg.key.nr_items;
            self.finish = self.nrest_item <= 512;
            self.pos = 0;
            Ok(())
        }
        pub fn new(sv2_arg: &'arg mut Sv2Args, fd: F) -> Result<Self, Errno> {
            sv2_arg.key.nr_items = u32::MAX;
            sv2_arg.key.min_offset = 0;
            // other fields not reset, maybe error?
            let mut ret = Self {
                sv2_arg,
                fd,
                pos: 0,
                offset: 0,
                nrest_item: 0,
                finish: false,
            };
            ret.call_ioctl()?;
            Ok(ret)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ExtentStat {
    pub disk: u64,
    pub uncomp: u64,
    pub refd: u64,
}

#[derive(Debug)]
struct Compsize {
    nfile: u64,
    ninline: u64,
    nref: u64,
    prealloc: ExtentStat,
    stat: [ExtentStat; 4],
    extents: HashSet<u64>,
}

#[derive(Debug)]
struct CompsizeStat {
    nfile: u64,
    ninline: u64,
    nref: u64,
    nextent: u64,
    prealloc: ExtentStat,
    stat: [ExtentStat; 4],
}

impl Compsize {
    fn new() -> Self {
        Self {
            nfile: 0,
            ninline: 0,
            nref: 0,
            stat: [ExtentStat {
                disk: 0,
                uncomp: 0,
                refd: 0,
            }; 4],
            prealloc: ExtentStat {
                disk: 0,
                uncomp: 0,
                refd: 0,
            },
            extents: HashSet::new(),
        }
    }
    fn add_file(&mut self, file_stat: &FileStat) {
        // TODO: refactor file stat: seperate inline and others
        let (key, comp, stat) = file_stat;
        match key.r#type() {
            btrfs::ExtentType::Inline => {
                self.ninline += 1;
                self.stat[comp.as_usize()].disk += stat.disk;
                self.stat[comp.as_usize()].uncomp += stat.uncomp;
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            btrfs::ExtentType::Regular => {
                self.nref += 1;
                if self.extents.insert(key.key()) {
                    self.stat[comp.as_usize()].disk += stat.disk;
                    self.stat[comp.as_usize()].uncomp += stat.uncomp;
                }
                self.stat[comp.as_usize()].refd += stat.refd;
            }
            btrfs::ExtentType::Prealloc => {
                self.nref += 1;
                if self.extents.insert(key.key()) {
                    self.prealloc.disk += stat.disk;
                    self.prealloc.uncomp += stat.uncomp;
                }
                self.prealloc.refd += stat.refd;
            }
        }
    }
    fn build_final(self) -> CompsizeStat {
        CompsizeStat {
            nfile: self.nfile,
            ninline: self.ninline,
            nref: self.nref,
            nextent: self.extents.len() as u64,
            prealloc: self.prealloc,
            stat: self.stat,
        }
    }
}

type FileStat = (ExtentKey, Compression, ExtentStat);

fn do_file(ws: &mut Compsize, fd: impl AsFd, ino: u64, filename: Display) {
    ws.nfile += 1;
    let mut sv2_args = btrfs::Sv2Args::new();
    match sv2_args.search_file(fd, ino) {
        Ok(iter) => iter
            .map(|item| match item.parse() {
                Ok(o) => o,
                Err(e) => die!("{}: {}", filename, e),
            })
            .for_each(|item| ws.add_file(&item)),
        Err(e) => {
            if e == Errno::NOTTY {
                die!("{}: Not btrfs (or SEARCH_V2 unsupported).", filename)
            } else {
                die!("{}: SEARCH_V2: {}", filename, e)
            }
        }
    }
}

fn search_path(ws: &mut Compsize, path: &mut PathBuf, dev: Dev) {
    let st = path.symlink_metadata().unwrap();
    // let st = path.metadata().unwrap();
    if st.is_dir() {
        path.read_dir().unwrap().for_each(|entry| {
            let entry = entry.unwrap();
            if entry.file_name() == "." || entry.file_name() == ".." {
                return;
            }
            path.push(entry.file_name());
            search_path(ws, path, st.st_dev());
            path.pop();
        });
    } else if st.is_file() {
        let file = match OpenOptions::new()
            .read(true)
            .write(false)
            .custom_flags(O_NOFOLLOW | O_NOCTTY | O_NONBLOCK)
            .open(&path)
        {
            Ok(f) => f,
            Err(e) => die!("open(\"{}\"): {}", path.display(), e),
        };
        do_file(ws, file, st.st_ino(), path.display());
    } else {
        // die!("{}: Not a file or directory.", path.display())
    }
}
fn main() {
    println!("Hello, world!");
    let mut ws = Compsize::new();
    for arg in args().skip(1) {
        let mut path = PathBuf::from(arg);
        let dev = path.metadata().unwrap().st_dev();
        search_path(&mut ws, &mut path, Dev::from(dev));
    }
    let final_stat = ws.build_final();
    println!("{:#?}", final_stat);
}

// fn parse_file_extent_item(buf: &[u8], hlen: u32, ws: &mut WorkSpace, file_name: &str) {
//     let extent_item = btrfs::FileExtentItem::from_bytes(buf);
//     if extent_item.get_type() == btrfs::BTRFS_FILE_EXTENT_INLINE {
//         do_inline(extent_item, hlen, ws);
//         return;
//     }
//     if hlen != 53 {
//         die!(
//             "{}: Regular extent's header not 53 bytes ({}) long?!?",
//             file_name,
//             hlen,
//         );
//     }
//     if extent_item.is_hole() {
//         return;
//     }
//     let disk_bytenr = extent_item.get_disk_bytenr();
//     if disk_bytenr.trailing_zeros() < 12 {
//         die!("{}: Extent not 4K-aligned at {}?!?", file_name, disk_bytenr)
//     }
//     let disk_bytenr = disk_bytenr >> 12;
//     let stat = if extent_item.get_type() == BTRFS_FILE_EXTENT_PREALLOC {
//         &mut ws.prealloc
//     } else {
//         &mut ws.stat[extent_item.get_compression() as usize]
//     };
//     if ws.seen_extents.insert(disk_bytenr) {
//         stat.disk += extent_item.get_disk_num_bytes() as u64;
//         stat.uncomp += extent_item.get_ram_bytes() as u64;
//         ws.nextents += 1;
//     }
//     stat.refd += extent_item.get_num_bytes() as u64;
//     ws.nrefs += 1;
// }

// fn do_inline(extent_item: btrfs::FileExtentItem, hlen: u32, ws: &mut WorkSpace) {
//     const INLINE_HEADER_SIZE: u32 = 21;
//     let disk_num_bytes = hlen - INLINE_HEADER_SIZE;
//     let ram_bytes = extent_item.get_ram_bytes() as u64;
//     ws.ninline += 1;
//     let stat = &mut ws.stat[extent_item.get_compression() as usize];
//     stat.disk += disk_num_bytes as u64;
//     stat.uncomp += ram_bytes;
//     stat.refd += ram_bytes;
// }
