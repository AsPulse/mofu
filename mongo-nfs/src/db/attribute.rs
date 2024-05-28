use mongodb::bson::oid::ObjectId;
use mongodb::bson::DateTime;
use nfsserve::nfs::{fattr3, fileid3, ftype3, gid3, mode3, size3, specdata3, uid3};
use serde::{Deserialize, Serialize};

use crate::db::time::MongoNFSTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct MofuAttribute {
    #[serde(skip_serializing)]
    pub _id: Option<ObjectId>,
    pub parent: ObjectId,
    pub name: String,
    pub is_dir: bool,
    pub payload: Option<MofuPayload>,
    pub uid: uid3,
    pub gid: gid3,
    pub mode: mode3,
    pub size: size3,
    pub accessed_at: MongoNFSTime,
    pub modified_at: MongoNFSTime,
    pub created_at: MongoNFSTime,
    pub timestamp: DateTime,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(crate) enum MofuPayload {
    InternalChunk {
        buffer_kb: u16,
    },
}

impl MofuAttribute {
    pub(crate) fn fattr3(&self, id: fileid3) -> fattr3 {
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
