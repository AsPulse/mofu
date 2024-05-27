use core::panic;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future;
use futures::StreamExt;
use itertools::Itertools;
use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::DateTime;
use mongodb::options::FindOptions;
use nfsserve::nfs::fattr3;
use nfsserve::nfs::fileid3;
use nfsserve::nfs::filename3;
use nfsserve::nfs::ftype3;
use nfsserve::nfs::nfspath3;
use nfsserve::nfs::nfsstat3;
use nfsserve::nfs::nfstime3;
use nfsserve::nfs::sattr3;
use nfsserve::nfs::specdata3;
use nfsserve::vfs::DirEntry;
use nfsserve::vfs::ReadDirResult;
use nfsserve::vfs::{NFSFileSystem, VFSCapabilities};
use thiserror::Error;
use tracing::error;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use crate::config::Config;
use crate::db::attribute::MofuAttribute;
use crate::db::time::MongoNFSTime;
use crate::db::MongoDB;
use fileid::{FileId, FileIdMap};

use self::mountpoint::MountPointInitializeError;
use self::mountpoint::MountpointId;
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

impl VFSMofuFS {
    async fn lookup_db(
        &self,
        db: &MongoDB,
        parent: ObjectId,
        fs_id: MountpointId,
        name: String,
    ) -> Result<fileid3, nfsstat3> {
        match db
            .attributes
            .find_one(
                doc! {
                    "parent": parent,
                    "name": name,
                },
                None,
            )
            .await
        {
            Ok(Some(doc)) => Ok(self
                .id_map
                .get_fileid_or_insert(FileId::ObjectId(fs_id, doc._id.unwrap()))),
            Ok(None) => Err(nfsstat3::NFS3ERR_NOENT),
            Err(e) => {
                error!("failed: {:?}", e);
                Err(nfsstat3::NFS3ERR_IO)
            }
        }
    }

    async fn readdir_db(
        &self,
        db: &MongoDB,
        parent: ObjectId,
        fs_id: MountpointId,
        start_after: Option<ObjectId>,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        let mut cursor = db
            .attributes
            .find(
                match start_after {
                    Some(o) => doc!{
                        "_id": {
                            "$gt": o,
                        },
                        "parent": parent,
                    },
                    None => doc! {
                        "parent": parent,
                    }
                },
                FindOptions::builder()
                    .sort(doc! {
                        "_id": 1
                    })
                    .limit((max_entries as i64).saturating_add(3))
                    .build(),
            )
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;

        let entries = cursor
            .by_ref()
            .take(max_entries)
            .map(|doc| {
                doc.map(|doc| {
                    let name = doc.name.clone().as_bytes().into();
                    let fileid = self
                        .id_map
                        .get_fileid_or_insert(FileId::ObjectId(fs_id, doc._id.unwrap()));
                    let attr = doc.fattr3(fileid);
                    DirEntry { fileid, name, attr }
                })
                .map_err(|e| {
                    error!("failed: {:?}", e);
                    nfsstat3::NFS3ERR_IO
                })
            })
            .collect::<Vec<Result<DirEntry, nfsstat3>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<DirEntry>, nfsstat3>>()?;

        let end = cursor.next().await.is_none();
        Ok(ReadDirResult { end, entries })
    }

    async fn mkdir_db(
        &self,
        db: &MongoDB,
        parent: ObjectId,
        fs_id: MountpointId,
        name: String,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let id = ObjectId::new();
        let doc = MofuAttribute {
            _id: None,
            parent,
            name,
            is_dir: true,
            mode: 777,
            uid: 0,
            gid: 0,
            size: 0,
            created_at: MongoNFSTime::now(),
            modified_at: MongoNFSTime::now(),
            accessed_at: MongoNFSTime::now(),
            timestamp: DateTime::now(),
        };

        db.attributes.insert_one(&doc, None).await.map_err(|e| {
            error!("failed: {:?}", e);
            nfsstat3::NFS3ERR_IO
        })?;
        let id = self
            .id_map
            .get_fileid_or_insert(FileId::ObjectId(fs_id, id));
        Ok((id, doc.fattr3(id)))
    }

    fn getdirectory_db(
        &self,
        dirid: fileid3,
    ) -> Result<(MountpointId, MongoDB, ObjectId), Option<FileId>> {
        match self.id_map.get_fileid(dirid) {
            Some(FileId::FileSystemRoot(fsid)) => {
                let mp = self.mountpoint.get(fsid);
                Ok((fsid, mp.db.clone(), mp.bucket))
            }
            Some(FileId::ObjectId(fsid, id)) => {
                let mp = self.mountpoint.get(fsid);
                Ok((fsid, mp.db.clone(), id))
            }
            o => Err(o),
        }
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
                info!("looking fs_id: {:?}", fs_id);
                if filename == ".." {
                    return Ok(self.root_dir());
                }
                let fs = self.mountpoint.get(fs_id);
                self.lookup_db(&fs.db, fs.bucket, fs_id, filename).await
            }
            Some(FileId::ObjectId(fs_id, obj_id)) => {
                info!("looking fs_id: {:?}", fs_id);
                if filename == ".." {
                    return Ok(self.root_dir());
                }
                let fs = self.mountpoint.get(fs_id);
                self.lookup_db(&fs.db, obj_id, fs_id, filename).await
            }
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Returns the attributes of an id.
    /// This method should be fast as it is used very frequently.
    #[instrument(name = "vfs/getattr", skip_all, fields(id = %id))]
    async fn getattr(&self, id: fileid3) -> Result<fattr3, nfsstat3> {
        match self.id_map.get_fileid(id) {
            Some(FileId::Root) | Some(FileId::FileSystemRoot(_)) => Ok(generate_default_dir(id)),
            Some(FileId::ObjectId(fs_id, obj_id)) => {
                let fs = self.mountpoint.get(fs_id);
                match fs
                    .db
                    .attributes
                    .find_one(
                        doc! {
                            "_id": obj_id,
                        },
                        None,
                    )
                    .await
                {
                    Ok(Some(doc)) => Ok(doc.fattr3(id)),
                    Ok(None) => Err(nfsstat3::NFS3ERR_NOENT),
                    Err(e) => {
                        error!("failed: {:?}", e);
                        Err(nfsstat3::NFS3ERR_IO)
                    }
                }
            }
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
    #[instrument(name = "vfs/mkdir", skip_all, fields(dir = %dirid, dirname = %dirname))]
    async fn mkdir(
        &self,
        dirid: fileid3,
        dirname: &filename3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        info!("mkdir",);
        match self.id_map.get_fileid(dirid) {
            Some(FileId::Root) => Err(nfsstat3::NFS3ERR_PERM),
            Some(FileId::FileSystemRoot(fs_id)) => {
                let name = String::from_utf8(dirname.0.clone()).unwrap();
                let mp = self.mountpoint.get(fs_id);
                self.mkdir_db(&mp.db, mp.bucket, fs_id, name).await
            }
            Some(FileId::ObjectId(fs_id, obj_id)) => {
                let name = String::from_utf8(dirname.0.clone()).unwrap();
                let mp = self.mountpoint.get(fs_id);
                self.mkdir_db(&mp.db, obj_id, fs_id, name).await
            }
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
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
    #[instrument(name = "vfs/rename", skip_all, fields(from_dir = %from_dirid, from_filename = %from_filename, to_dir = %to_dirid, to_filename = %to_filename))]
    async fn rename(
        &self,
        from_dirid: fileid3,
        from_filename: &filename3,
        to_dirid: fileid3,
        to_filename: &filename3,
    ) -> Result<(), nfsstat3> {
        let from_dir = self.id_map.get_fileid(from_dirid);
        let to_dir = self.id_map.get_fileid(to_dirid);

        let (from_fsid, db, from_objid) = match self.getdirectory_db(from_dirid) {
            Ok(o) => o,
            Err(Some(FileId::Root)) => {
                error!("cannot rename root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            Err(None) => return Err(nfsstat3::NFS3ERR_NOENT),
            _ => panic!("Unexpected getdirectory_db result."),
        };

        let (to_fsid, _, to_objid) = match self.getdirectory_db(to_dirid) {
            Ok(o) => o,
            Err(Some(FileId::Root)) => {
                error!("cannot rename root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            Err(None) => return Err(nfsstat3::NFS3ERR_NOENT),
            _ => panic!("Unexpected getdirectory_db result."),
        };

        if from_fsid != to_fsid {
            // TODO: Implement rename across filesystems
            error!("rename across filesystems is currently not supported.");
            return Err(nfsstat3::NFS3ERR_NOTSUPP);
        }

        db.attributes
            .update_one(
                doc! {
                "parent": from_objid,
                "name": String::from_utf8(from_filename.0.clone()).unwrap(),
                },
                doc! {
                    "$set": {
                        "parent": to_objid,
                        "name": String::from_utf8(to_filename.0.clone()).unwrap(),
                    }
                },
                None,
            )
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;
        Ok(())
    }

    /// Returns the contents of a directory with pagination.
    /// Directory listing should be deterministic.
    /// Up to max_entries may be returned, and start_after is used
    /// to determine where to start returning entries from.
    ///
    /// For instance if the directory has entry with ids [1,6,2,11,8,9]
    /// and start_after=6, readdir should returning 2,11,8,...
    //
    #[instrument(name = "vfs/readdir", skip_all, fields(dir = %dirid, start_after = %start_after))]
    async fn readdir(
        &self,
        dirid: fileid3,
        start_after: fileid3,
        max_entries: usize,
    ) -> Result<ReadDirResult, nfsstat3> {
        match self.id_map.get_fileid(dirid) {
            Some(FileId::Root) => {
                let mut original = self
                    .mountpoint
                    .id_map
                    .iter()
                    .map(|(name, id)| {
                        (
                            name,
                            self.id_map.get_u64(&FileId::FileSystemRoot(*id)).unwrap(),
                        )
                    })
                    .skip_while(|&(_, id)| id <= start_after);
                let entries = original
                    .by_ref()
                    .take(max_entries)
                    .map(|(name, id)| DirEntry {
                        fileid: id,
                        name: name.as_bytes().into(),
                        attr: generate_default_dir(id),
                    })
                    .collect_vec();
                let end = original.next().is_none();
                Ok(ReadDirResult { end, entries })
            }
            Some(FileId::FileSystemRoot(fs_id)) => {
                info!("reading fs_id: {:?}", dirid);
                let id = if start_after == 0 {
                    None
                } else {
                    match self.id_map.get_fileid(start_after) {
                        Some(FileId::ObjectId(fs, id)) if fs == fs_id => Some(id),
                        _ => None,
                    }
                };
                let mp = self.mountpoint.get(fs_id);
                self.readdir_db(&mp.db, mp.bucket, fs_id, id, max_entries)
                    .await
            }
            Some(FileId::ObjectId(fs_id, obj_id)) => {
                info!("reading fs_id: {:?}", fs_id);
                let id = if start_after == 0 {
                    None
                } else {
                    match self.id_map.get_fileid(start_after) {
                        Some(FileId::ObjectId(fs, id)) if fs == fs_id => Some(id),
                        _ => None,
                    }
                };
                let mp = self.mountpoint.get(fs_id);
                self.readdir_db(&mp.db, obj_id, fs_id, id, max_entries)
                    .await
            }
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Makes a symlink with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[instrument(name = "vfs/symlink", skip_all, fields(dir = %_dirid, linkname = %_linkname, symlink = %_symlink))]
    async fn symlink(
        &self,
        _dirid: fileid3,
        _linkname: &filename3,
        _symlink: &nfspath3,
        _attr: &sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        warn!("symlink not supported");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }

    /// Reads a symlink
    #[instrument(name = "vfs/readlink", skip_all, fields(id = %_id))]
    async fn readlink(&self, _id: fileid3) -> Result<nfspath3, nfsstat3> {
        warn!("readlink not supported");
        Err(nfsstat3::NFS3ERR_NOTSUPP)
    }
}
