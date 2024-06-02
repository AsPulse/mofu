use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use itertools::Itertools;
use mongodb::bson::{doc, oid::ObjectId, DateTime, Document};
use mongodb::options::{FindOneAndUpdateOptions, FindOptions, ReturnDocument};
use nfsserve::nfs::set_uid3;
use nfsserve::nfs::specdata3;
use nfsserve::nfs::{
    fattr3, fileid3, filename3, ftype3, nfspath3, nfsstat3, nfstime3, sattr3, set_atime, set_gid3,
    set_mode3, set_mtime, set_size3,
};
use nfsserve::vfs::{DirEntry, NFSFileSystem, ReadDirResult, VFSCapabilities};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info, instrument, warn};

use crate::config::Config;
use crate::db::attribute::MofuAttribute;
use crate::db::attribute::MofuPayload;
use crate::db::time::MongoNFSTime;
use crate::db::util::{to_bson_and_err, u64_to_bson_and_err};
use crate::db::MongoDB;
use fileid::{FileId, FileIdMap};

use self::mongorw::{run_mongorw, MongoRw};
use self::mountpoint::{MountPointInitializeError, MountpointId, MountpointMap};
mod fileid;
pub mod mongorw;
mod mountpoint;

pub struct VFSMofuFS {
    pub config: Config,
    pub db: Arc<BTreeMap<String, Arc<MongoDB>>>,
    id_map: FileIdMap,
    mountpoint: MountpointMap,
    tx_mongorw: mpsc::Sender<MongoRw>,
}

#[derive(Error, Debug)]
pub enum MofuInitializeError {
    #[error("failed to initialize mountpoint: {0}")]
    MountPointInitializeError(#[from] MountPointInitializeError),
}

impl VFSMofuFS {
    pub async fn new(
        config: Config,
        db: Arc<BTreeMap<String, Arc<MongoDB>>>,
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
            tx_mongorw: run_mongorw().await,
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

fn sattr3_doc(attr: &sattr3) -> Result<Document, nfsstat3> {
    let mut doc = doc! {};
    if let set_mode3::mode(mode) = attr.mode {
        doc.insert("mode", mode);
    }
    if let set_uid3::uid(uid) = attr.uid {
        doc.insert("uid", uid);
    }
    if let set_gid3::gid(gid) = attr.gid {
        doc.insert("gid", gid);
    }
    if let set_size3::size(size) = attr.size {
        doc.insert("size", u64_to_bson_and_err(size)?);
    }
    match attr.atime {
        set_atime::DONT_CHANGE => {}
        set_atime::SET_TO_SERVER_TIME => {
            doc.insert("accessed_at", to_bson_and_err(&MongoNFSTime::now())?);
        }
        set_atime::SET_TO_CLIENT_TIME(time) => {
            doc.insert("accessed_at", to_bson_and_err(&MongoNFSTime::from(time))?);
        }
    }
    match attr.mtime {
        set_mtime::DONT_CHANGE => {}
        set_mtime::SET_TO_SERVER_TIME => {
            doc.insert("modified_at", to_bson_and_err(&MongoNFSTime::now())?);
        }
        set_mtime::SET_TO_CLIENT_TIME(time) => {
            doc.insert("modified_at", to_bson_and_err(&MongoNFSTime::from(time))?);
        }
    }

    Ok(doc)
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
                    Some(o) => doc! {
                        "_id": {
                            "$gt": o,
                        },
                        "parent": parent,
                    },
                    None => doc! {
                        "parent": parent,
                    },
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
            payload: None,
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

    fn getdirectory_db(&self, dirid: fileid3) -> DbDirectory {
        match self.id_map.get_fileid(dirid) {
            Some(FileId::FileSystemRoot(fsid)) => {
                let mp = self.mountpoint.get(fsid);
                DbDirectory::Found(fsid, mp.db.clone(), mp.bucket)
            }
            Some(FileId::ObjectId(fsid, id)) => {
                let mp = self.mountpoint.get(fsid);
                DbDirectory::Found(fsid, mp.db.clone(), id)
            }
            Some(FileId::Root) => DbDirectory::Root,
            None => DbDirectory::NotFound,
        }
    }
}

pub enum DbDirectory {
    Found(MountpointId, Arc<MongoDB>, ObjectId),
    Root,
    NotFound,
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
                let (tx, rx) = oneshot::channel();
                self.tx_mongorw
                    .send(MongoRw::GetAttr {
                        object_id: obj_id,
                        db: fs.db.clone(),
                        reply: tx,
                    })
                    .await
                    .map_err(|e| {
                        error!("failed: {:?}", e);
                        nfsstat3::NFS3ERR_IO
                    })?;
                let attr = rx.await.map_err(|e| {
                    error!("receiving mongo_rw failed: {:?}", e);
                    nfsstat3::NFS3ERR_SERVERFAULT
                })??;
                info!("getattr: size={:?}", attr.size);
                Ok(attr.fattr3(id))
            }
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Sets the attributes of an id
    /// this should return Err(nfsstat3::NFS3ERR_ROFS) if readonly
    #[instrument(name = "vfs/setattr", skip_all, fields(id = %id, setattr = ?setattr))]
    async fn setattr(&self, id: fileid3, setattr: sattr3) -> Result<fattr3, nfsstat3> {
        let (fsid, objid) = match self.id_map.get_fileid(id) {
            Some(FileId::ObjectId(fsid, objid)) => (fsid, objid),
            Some(FileId::FileSystemRoot(_)) | Some(FileId::Root) => {
                error!("cannot write to root or filesystem root");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            _ => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let mp = self.mountpoint.get(fsid);
        let result = mp
            .db
            .attributes
            .find_one_and_update(
                doc! {
                    "_id": objid,
                },
                doc! {
                    "$set": sattr3_doc(&setattr)?
                },
                None,
            )
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;
        match result {
            Some(doc) => Ok(doc.fattr3(id)),
            None => Err(nfsstat3::NFS3ERR_NOENT),
        }
    }

    /// Reads the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, all bytes till the end of file are returned.
    /// EOF must be flagged if the end of the file is reached by the read.
    #[instrument(name = "vfs/read", skip_all, fields(id = %id, offset = %offset, count = %count))]
    async fn read(
        &self,
        id: fileid3,
        offset: u64,
        count: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        info!("reading file");
        let (fsid, objid) = match self.id_map.get_fileid(id) {
            Some(FileId::ObjectId(fsid, objid)) => (fsid, objid),
            Some(FileId::FileSystemRoot(_)) | Some(FileId::Root) => {
                error!("cannot read  root or filesystem root");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            _ => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let mp = self.mountpoint.get(fsid);

        let (tx, rx) = oneshot::channel();
        self.tx_mongorw
            .send(MongoRw::Read {
                object_id: objid,
                db: mp.db.clone(),
                offset,
                size: count,
                reply: tx,
            })
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;

        rx.await.map_err(|e| {
            error!("receiving mongo_rw failed: {:?}", e);
            nfsstat3::NFS3ERR_SERVERFAULT
        })?
    }

    /// Writes the contents of a file returning (bytes, EOF)
    /// Note that offset/count may go past the end of the file and that
    /// in that case, the file is extended.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[instrument(name = "vfs/write", skip_all, fields(id = %id, offset = %offset, data_len = %data.len()))]
    async fn write(&self, id: fileid3, offset: u64, data: &[u8]) -> Result<fattr3, nfsstat3> {
        let start = Instant::now();
        info!("writing file");
        let (fsid, objid) = match self.id_map.get_fileid(id) {
            Some(FileId::ObjectId(fsid, objid)) => (fsid, objid),
            Some(FileId::FileSystemRoot(_)) | Some(FileId::Root) => {
                error!("cannot write to root or filesystem root");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            _ => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let mp = self.mountpoint.get(fsid);
        let from_tx = Instant::now();
        let (tx, rx) = oneshot::channel();
        self.tx_mongorw
            .send(MongoRw::Write {
                object_id: objid,
                db: mp.db.clone(),
                offset,
                data: data.to_vec(),
                reply: tx,
            })
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?;

        let from_rx = Instant::now();
        let ret = rx
            .await
            .map_err(|e| {
                error!("receiving mongo_rw failed: {:?}", e);
                nfsstat3::NFS3ERR_SERVERFAULT
            })?
            .map(|doc| doc.fattr3(id));

        info!(
            "write: {:?}, (tx: {:?}, rx: {:?})",
            start.elapsed(),
            from_tx.elapsed(),
            from_rx.elapsed()
        );
        ret
    }

    /// Creates a file with the following attributes.
    /// If not supported due to readonly file system
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[instrument(name = "vfs/create", skip_all, fields(dir = %dirid, filename = %filename))]
    async fn create(
        &self,
        dirid: fileid3,
        filename: &filename3,
        attr: sattr3,
    ) -> Result<(fileid3, fattr3), nfsstat3> {
        let (fsid, db, id) = match self.getdirectory_db(dirid) {
            DbDirectory::Found(f, d, o) => (f, d, o),
            DbDirectory::Root => {
                error!("cannot create file/directory in root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            DbDirectory::NotFound => return Err(nfsstat3::NFS3ERR_NOENT),
        };
        info!("creating file: {:?}", attr);

        let query = doc! {
            "name": String::from_utf8(filename.0.clone()).unwrap(),
            "parent": id,
        };

        let sattr3 = sattr3_doc(&attr)?;

        let mut insert = doc! {
            "is_dir": false,
            "payload": to_bson_and_err(&None::<MofuPayload>)?,
            "created_at": to_bson_and_err(&MongoNFSTime::now())?, "timestamp": DateTime::now(),
        };

        if !sattr3.contains_key("uid") {
            insert.insert("uid", 0);
        }
        if !sattr3.contains_key("gid") {
            insert.insert("gid", 0);
        }
        if !sattr3.contains_key("mode") {
            insert.insert("mode", 777);
        }
        if !sattr3.contains_key("size") {
            insert.insert("size", 0);
        }
        if !sattr3.contains_key("accessed_at") {
            insert.insert("accessed_at", to_bson_and_err(&MongoNFSTime::now())?);
        }
        if !sattr3.contains_key("modified_at") {
            insert.insert("modified_at", to_bson_and_err(&MongoNFSTime::now())?);
        }

        let doc = doc! {
            "$setOnInsert": insert,
            "$set": sattr3,
        };

        let update = db
            .attributes
            .find_one_and_update(
                query,
                doc,
                FindOneAndUpdateOptions::builder()
                    .upsert(true)
                    .return_document(ReturnDocument::After)
                    .build(),
            )
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?
            .ok_or_else(|| {
                error!("failed to create file");
                nfsstat3::NFS3ERR_IO
            })?;

        let id = update._id.unwrap();
        let file_id = self.id_map.get_fileid_or_insert(FileId::ObjectId(fsid, id));
        let attr = update.fattr3(file_id);

        Ok((file_id, attr))
    }

    /// Creates a file if it does not already exist
    /// this should return Err(nfsstat3::NFS3ERR_ROFS)
    #[instrument(name = "vfs/create_exclusive", skip_all, fields(dir = %dirid, filename = %filename))]
    async fn create_exclusive(
        &self,
        dirid: fileid3,
        filename: &filename3,
    ) -> Result<fileid3, nfsstat3> {
        info!("creating file");
        let (fsid, db, id) = match self.getdirectory_db(dirid) {
            DbDirectory::Found(f, d, o) => (f, d, o),
            DbDirectory::Root => {
                error!("cannot create file/directory in root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            DbDirectory::NotFound => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let attr = MofuAttribute {
            _id: None,
            parent: id,
            name: String::from_utf8(filename.0.clone()).unwrap(),
            payload: None,
            is_dir: false,
            mode: 777,
            uid: 0,
            gid: 0,
            size: 0,
            created_at: MongoNFSTime::now(),
            modified_at: MongoNFSTime::now(),
            accessed_at: MongoNFSTime::now(),
            timestamp: DateTime::now(),
        };

        let oid = db
            .attributes
            .insert_one(&attr, None)
            .await
            .map_err(|e| {
                match *e.kind {
                    // WARN: この判定手法があってるかわからん
                    // (既にファイルが存在した場合のErrorKindが何になるか調べないと……)
                    mongodb::error::ErrorKind::Write(_) => nfsstat3::NFS3ERR_EXIST,
                    _ => {
                        error!("failed: {:?}", e);
                        nfsstat3::NFS3ERR_IO
                    }
                }
            })?
            .inserted_id
            .as_object_id()
            .unwrap();

        let fileid = self
            .id_map
            .get_fileid_or_insert(FileId::ObjectId(fsid, oid));
        Ok(fileid)
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
    #[instrument(name = "vfs/remove", skip_all, fields(dir = %dirid, filename = %filename))]
    async fn remove(&self, dirid: fileid3, filename: &filename3) -> Result<(), nfsstat3> {
        info!("removing file");
        let (_, db, id) = match self.getdirectory_db(dirid) {
            DbDirectory::Found(f, d, o) => (f, d, o),
            DbDirectory::Root => {
                error!("cannot remove root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            DbDirectory::NotFound => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let attr = db
            .attributes
            .find_one_and_delete(
                doc! {
                    "parent": id,
                    "name": String::from_utf8(filename.0.clone()).unwrap(),
                },
                None,
            )
            .await
            .map_err(|e| {
                error!("failed: {:?}", e);
                nfsstat3::NFS3ERR_IO
            })?
            .ok_or_else(|| {
                warn!("file not found");
                nfsstat3::NFS3ERR_NOENT
            })?;

        if !attr.is_dir {
            db.chunks
                .delete_many(
                    doc! {
                        "file": attr._id.unwrap(),
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    error!("failed: {:?}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
        }
        Ok(())
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
        let (from_fsid, db, from_objid) = match self.getdirectory_db(from_dirid) {
            DbDirectory::Found(f, d, o) => (f, d, o),
            DbDirectory::Root => {
                error!("cannot rename root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            DbDirectory::NotFound => return Err(nfsstat3::NFS3ERR_NOENT),
        };

        let (to_fsid, _, to_objid) = match self.getdirectory_db(to_dirid) {
            DbDirectory::Found(f, d, o) => (f, d, o),
            DbDirectory::Root => {
                error!("cannot rename root. change mountpoint config instead.");
                return Err(nfsstat3::NFS3ERR_PERM);
            }
            DbDirectory::NotFound => return Err(nfsstat3::NFS3ERR_NOENT),
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
