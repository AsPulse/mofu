use nfsserve::nfs::nfstime3;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Default, Serialize, Deserialize)]
#[repr(C)]
pub struct MongoNFSTime {
    pub seconds: u32,
    pub nseconds: u32,
}
impl MongoNFSTime {
    pub fn now() -> Self {
        let now = std::time::SystemTime::now();
        let duration = now.duration_since(std::time::UNIX_EPOCH).unwrap();
        Self {
            seconds: duration.as_secs() as u32,
            nseconds: duration.subsec_nanos(),
        }
    }
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
