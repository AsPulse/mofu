use mongodb::bson::to_bson;
use nfsserve::nfs::nfsstat3;
use serde::Serialize;
use std::fmt::Debug;
use tracing::error;

pub fn to_bson_and_err<T: Serialize + Debug>(value: &T) -> Result<mongodb::bson::Bson, nfsstat3> {
    match to_bson(value) {
        Ok(bson) => Ok(bson),
        Err(e) => {
            error!("failed to serialize value {:?}: {:?}", value, e);
            Err(nfsstat3::NFS3ERR_IO)
        }
    }
}
