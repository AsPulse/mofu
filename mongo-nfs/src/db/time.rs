use nfsserve::nfs::nfstime3;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
#[repr(C)]
pub struct MongoNFSTime {
    pub seconds: u32,
    pub nseconds: u32,
}
impl From<nfstime3> for MongoNFSTime {
    fn from(t: nfstime3) -> Self {
        Self {
            seconds: t.seconds,
            nseconds: t.nseconds,
        }
    }
}
impl From<MongoNFSTime> for nfstime3 {
    fn from(t: MongoNFSTime) -> Self {
        Self {
            seconds: t.seconds,
            nseconds: t.nseconds,
        }
    }
}
