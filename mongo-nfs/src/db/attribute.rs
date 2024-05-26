use mongodb::bson::oid::ObjectId;
use nfsserve::nfs::{fattr3, fileid3, ftype3, gid3, mode3, size3, specdata3, uid3};
use serde::{Deserialize, Serialize};

use crate::db::time::MongoNFSTime;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MofuAttribute {
    #[serde(skip_serializing)]
    _id: Option<ObjectId>,
    parent: ObjectId,
    is_dir: bool,
    uid: uid3,
    gid: gid3,
    mode: mode3,
    size: size3,
    accessed_at: MongoNFSTime,
    modified_at: MongoNFSTime,
    created_at: MongoNFSTime,
}

impl MofuAttribute {
    pub(crate) fn fattr3(self, id: fileid3) -> fattr3 {
        fattr3 {
            ftype: if self.is_dir {
                ftype3::NF3DIR
            } else {
                ftype3::NF3REG
            },
            mode: self.mode,
            nlink: 0,
            uid: self.uid,
            gid: self.gid,
            size: self.size,
            used: self.size,
            rdev: specdata3::default(),
            fsid: 0,
            fileid: id,
            atime: self.accessed_at.into(),
            mtime: self.modified_at.into(),
            ctime: self.created_at.into(),
        }
    }
}
