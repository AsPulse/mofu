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

pub fn u64_to_bson_and_err(value: u64) -> Result<mongodb::bson::Bson, nfsstat3> {
    // https://docs.rs/bson/latest/src/bson/ser/serde.rs.html#221-228
    match i64::try_from(value) {
        Ok(ivalue) => to_bson_and_err(&ivalue),
        Err(_) => {
            error!("unsigned integer exceeded range: {}", value);
            Err(nfsstat3::NFS3ERR_NOTSUPP)
        }
    }
}
