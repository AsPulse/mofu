use std::ops::RangeBounds;

use mongodb::bson::spec::BinarySubtype;
use tracing::{error, info};

use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, Binary, Bson, DateTime};
use nfsserve::nfs::nfsstat3;
use serde::{Deserialize, Serialize};

use crate::db::attribute::MofuPayload;
use crate::db::util::to_bson_and_err;

use super::attribute::MofuAttribute;
use super::MongoDB;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MofuChunk {
    #[serde(skip_serializing)]
    pub _id: Option<ObjectId>,
    pub file: ObjectId,
    pub index: u32,
    pub payload: Bson,
    pub timestamp: DateTime,
}

const KB_TO_BYTES: u64 = 1024;

/// FIXME: Inefficient implementation
/// When rewriting an entire range, there is no need to retrieve previous data or send requests one at a time.

async fn write_single_chunk<R: RangeBounds<usize>>(
    db: MongoDB,
    id: ObjectId,
    index: u32,
    chunk_size: u64,
    data: Vec<u8>,
    range: R,
) -> Result<(), nfsstat3> {
    let chunk = db
        .chunks
        .find_one(
            doc! {
                "file": id,
                "index": index
            },
            None,
        )
        .await
        .map_err(|e| {
            error!("failed: {:?}", e);
            nfsstat3::NFS3ERR_IO
        })?;

    match chunk {
        Some(c) => {
            let mut payload = match c.payload {
                Bson::Binary(b) => b.bytes,
                _ => {
                    error!("payload is not Binary");
                    return Err(nfsstat3::NFS3ERR_IO);
                }
            };
            payload.splice(range, data.iter().cloned());
            db.chunks
                .update_one(
                    doc! {
                        "_id": c._id
                    },
                    doc! {
                        "$set": {
                            "payload": Bson::Binary(Binary { subtype: BinarySubtype::Generic, bytes: payload }),
                        }
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    error!("failed: {:?}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
        }
        None => {
            let mut payload = vec![
                0u8;
                match range.end_bound() {
                    std::ops::Bound::Included(e) => *e + 1,
                    std::ops::Bound::Excluded(e) => *e,
                    std::ops::Bound::Unbounded => chunk_size as usize,
                }
            ];
            payload.splice(range, data.iter().cloned());
            db.chunks
                .insert_one(
                    MofuChunk {
                        _id: None,
                        file: id,
                        index,
                        payload: Bson::Binary(Binary {
                            subtype: BinarySubtype::Generic,
                            bytes: payload,
                        }),
                        timestamp: DateTime::now(),
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    error!("failed: {:?}", e);
                    nfsstat3::NFS3ERR_IO
                })?;
        }
    }
    Ok(())
}

impl MofuAttribute {
    pub async fn write_chunk(
        &self,
        db: MongoDB,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<(), nfsstat3> {
        assert!(!self.is_dir);
        let self_id = self._id.expect("self._id is None when writing to chunks");
        let payload = match self.payload.clone() {
            Some(p) => p,
            None => {
                let payload = MofuPayload::InternalChunk { buffer_kb: 1024 };
                info!("payload is None, creating new chunk: {:?}", payload);
                db.attributes
                    .update_one(
                        doc! {
                            "_id": self_id
                        },
                        doc! {
                            "$set": {
                                "payload": to_bson_and_err(&Some(payload.clone()))?
                            }
                        },
                        None,
                    )
                    .await
                    .map_err(|e| {
                        error!("failed: {:?}", e);
                        nfsstat3::NFS3ERR_IO
                    })?;
                payload
            }
        };

        match payload {
            MofuPayload::InternalChunk { buffer_kb } => {
                let chunk_size = buffer_kb as u64 * KB_TO_BYTES;

                let start_idx = u32::try_from(offset / chunk_size).unwrap();
                let start_pos = usize::try_from(offset % chunk_size).unwrap();

                let end_idx = u32::try_from((offset + data.len() as u64 - 1) / chunk_size).unwrap();
                let end_pos =
                    usize::try_from((offset + data.len() as u64 - 1) % chunk_size).unwrap();

                for i in start_idx..=end_idx {
                    let range = if i == start_idx && i == end_idx {
                        start_pos..=end_pos
                    } else if i == start_idx {
                        start_pos..=(chunk_size as usize - 1)
                    } else if i == end_idx {
                        0..=end_pos
                    } else {
                        0..=(chunk_size as usize - 1)
                    };
                    write_single_chunk(db.clone(), self_id, i, chunk_size, data.clone(), range)
                        .await?;
                }
            }
        }
        let end_at = offset + data.len() as u64;
        if self.size < end_at {
            info!("updating size: {:?} -> {:?}", self.size, end_at);
            let size = match i64::try_from(end_at) {
                Ok(ivalue) => ivalue,
                Err(_) => {
                    error!("failed to update size, value is too large: {:?}", end_at);
                    return Err(nfsstat3::NFS3ERR_IO);
                }
            };
            db.attributes
                .update_one(
                    doc! {
                        "_id": self_id
                    },
                    doc! {
                        "$set": {
                            "size": size,
                        }
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
}
