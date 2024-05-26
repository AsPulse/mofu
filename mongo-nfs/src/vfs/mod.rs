use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use itertools::Itertools;
use nfsserve::nfs::fattr3;
use nfsserve::nfs::fileid3;
use nfsserve::nfs::filename3;
use nfsserve::nfs::ftype3;
use nfsserve::nfs::nfspath3;
use nfsserve::nfs::nfsstat3;
use nfsserve::nfs::nfsstring;
use nfsserve::nfs::nfstime3;
use nfsserve::nfs::sattr3;
use nfsserve::nfs::specdata3;
use nfsserve::vfs::DirEntry;
use nfsserve::vfs::ReadDirResult;
use nfsserve::vfs::{NFSFileSystem, VFSCapabilities};
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelBridge;
use thiserror::Error;
use tracing::error;
use tracing::info;
use tracing::instrument;

use crate::config::Config;
use crate::db::MongoDB;
use fileid::{FileId, FileIdMap};

use self::mountpoint::MountPointInitializeError;
use self::mountpoint::MountpointMap;
mod fileid;
mod mountpoint;

pub struct VFSMofuFS {
    pub config: Config,
    pub db: Arc<BTreeMap<String, MongoDB>>,
    id_map: FileIdMap,
    mountpoint: MountpointMap,
}

#[derive(Error, Debug)]
pub enum MofuInitializeError {
    #[error("failed to initialize mountpoint: {0}")]
    MountPointInitializeError(#[from] MountPointInitializeError),
}

impl VFSMofuFS {
    pub async fn new(
        config: Config,
        db: Arc<BTreeMap<String, MongoDB>>,
    ) -> Result<Self, MofuInitializeError> {
        let id_map = FileIdMap::new();
        id_map.insert(FileId::Root);
        let mountpoint = MountpointMap::new(&config, &db).await?;
        mountpoint.id_map.right_values().for_each(|id| {
            id_map.insert(FileId::FileSystemRoot(*id));
        });

        Ok(Self {
            mountpoint,
            config,
            db,
            id_map,
        })
    }
}

fn generate_default_dir(id: fileid3) -> fattr3 {
    fattr3 {
        ftype: ftype3::NF3DIR,
        mode: 777,
        nlink: 0,
        uid: 0,
        gid: 0,
        size: 0,
        used: 0,
        rdev: specdata3::default(),
        fsid: 0,
        fileid: id,
        atime: nfstime3::default(),
        mtime: nfstime3::default(),
        ctime: nfstime3::default(),
    }
}

#[async_trait]
impl NFSFileSystem for VFSMofuFS {
    fn root_dir(&self) -> fileid3 {
        self.id_map.get_u64(&FileId::Root).unwrap()
    }

    fn capabilities(&self) -> VFSCapabilities {
        VFSCapabilities::ReadWrite
    }

    #[instrument(name = "vfs/lookup", skip_all, fields(dir = %dirid, filename = %filename))]
    async fn lookup(&self, dirid: fileid3, filename: &filename3) -> Result<fileid3, nfsstat3> {
        let filename = String::from_utf8(filename.0.clone()).unwrap();
        if filename == "." {
            return Ok(dirid);
        }
        match self.id_map.get_fileid(dirid) {
            Some(FileId::Root) => {
                if filename == ".." {
                    return Err(nfsstat3::NFS3ERR_NOENT);
                }
                if let Some(id) = self.mountpoint.id_map.get_by_left(&filename) {
                    Ok(self.id_map.get_u64(&FileId::FileSystemRoot(*id)).unwrap())
                } else {
                    info!("mountpoint not found");
                    Err(nfsstat3::NFS3ERR_NOENT)
                }
            }
            Some(FileId::FileSystemRoot(fs_id)) => {
                if filename == ".." {
                    return Ok(self.root_dir());
                }
                // TODO:
                error!("lookup not implemented for filesystem root.");
                return Err(nfsstat3::NFS3ERR_NOENT);
            }
            Some(FileId::ObjectId(fs_id, obj_id)) => todo!(),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Returns the attributes of an id.
    /// This method should be fast as it is used very frequently.
    #[instrument(name = "vfs/getattr", skip_all, fields(id = %id))]
    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match self.id_map.get_fileid(id) {
            Some(FileId::Root) | Some(FileId::FileSystemRoot(_)) => Ok(generate_default_dir(id)),
            Some(FileId::ObjectId(fs_id, obj_id)) => todo!(),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Sets the attributes of an id
    /// this should return Err(nfsstat3::NFS3ERR_ROFS) if readonly
    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        todo!()
    }

    /// Reads the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, all bytes till the end of file are returned.
    /// EOF must be flagged if the end of the file is reached by the read.
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        todo!()
    }

    /// Writes the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, the file is extended.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        todo!()
    }

    /// Creates a file with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        todo!()
    }

    /// Creates a file if it does not already exist
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        todo!()
    }

    /// Makes a directory with the following attributes.
    /// If not supported dur to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        todo!()
    }

    /// Removes a file.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        todo!()
    }

    /// Removes a file.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        todo!()
    }

    /// Returns the contents of a directory with pagination.
    /// Directory listing should be deterministic.
    /// Up to max_entries may be returned, and start_after is used
    /// to determine where to start returning entries from.
    ///
    /// For instance if the directory has entry with ids [1,6,2,11,8,9]
    /// and start_after=6, readdir should returning 2,11,8,...
    //
    #[instrument(name = "vfs/readdir", skip_all, fields(dir = %dirid))]
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        match self.id_map.get_fileid(dirid) {
            Some(FileId::Root) => {
                let result = self
                    .mountpoint
                    .id_map
                    .iter()
                    .map(|(name, id)| {
                        (
                            name,
                            self.id_map.get_u64(&FileId::FileSystemRoot(*id)).unwrap(),
                        )
                    })
                    .skip_while(|&(_, id)| id <= start_after)
                    .take(max_entries)
                    .map(|(name, id)| DirEntry {
                        fileid: id,
                        name: name.as_bytes().into(),
                        attr: generate_default_dir(id),
                    });
                let end = result.clone().next().is_none();
                let entries = result.collect_vec();
                Ok(ReadDirResult { end, entries })
            }
            Some(FileId::FileSystemRoot(_)) => {
                // TODO:
                error!("readdir not implemented for filesystem root.");
                Ok(ReadDirResult {
                    end: true,
                    entries: vec![],
                })
            }
            Some(FileId::ObjectId(fs_id, obj_id)) => todo!(),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Makes a symlink with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    async fn symlink(
        &self,
        dirid: fileid3,
        linkname: &filename3,
        symlink: &nfspath3,
        attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        todo!()
    }

    /// Reads a symlink
    async fn readlink(&self, id: fileid3) -> Result<nfspath3, nfsstat3> {
        todo!()
    }
}
