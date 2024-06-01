use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use mongodb::bson::oid::ObjectId;
use nfsserve::nfs::nfsstat3;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::db::attribute::MofuAttribute;
use crate::db::chunk::LocalChunk;
use crate::db::MongoDB;

pub enum MongoRw {
    GetAttr {
        object_id: ObjectId,
        db: Arc<MongoDB>,
        reply: oneshot::Sender<Result<MofuAttribute, nfsstat3>>,
    },
    Read {
        object_id: ObjectId,
        db: Arc<MongoDB>,
        offset: u64,
        size: u32,
        reply: oneshot::Sender<Result<(Vec<u8>, bool), nfsstat3>>,
    },
    Write {
        object_id: ObjectId,
        db: Arc<MongoDB>,
        offset: u64,
        data: Vec<u8>,
        reply: oneshot::Sender<Result<MofuAttribute, nfsstat3>>,
    },
}

pub(crate) async fn run_mongorw() -> mpsc::Sender<MongoRw> {
    let (tx, mut rx) = mpsc::channel::<MongoRw>(32);

    tokio::spawn(async move {
        info!("MongoRw started");
        let mut map = HashMap::<ObjectId, LocalChunk>::new();
        loop {
            for (_, chunk) in map.iter_mut() {
                let _ = chunk.commit_rest().await;
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                msg = rx.recv() => {
                    match msg {
                        Some(MongoRw::GetAttr { object_id, db, reply }) => {
                            reply.send(get_attr(object_id, db, &mut map).await).unwrap_or_else(|e| {
                                error!("Failed to send reply: {:?}", e);
                            })
                        },
                        Some(MongoRw::Read { object_id, db, offset, size, reply }) => {
                            reply.send(read(object_id, db, offset, size, &mut map).await).unwrap_or_else(|e| {
                                error!("Failed to send reply: {:?}", e);
                            })
                        },
                        Some(MongoRw::Write { object_id, db, offset, data, reply }) => {
                            reply.send(write(object_id, db, offset, data, &mut map).await).unwrap_or_else(|e| {
                                error!("Failed to send reply: {:?}", e);
                            })
                        },
                        None => break,
                    }
                }
            }
        }
    });
    tx
}
async fn write(
    object_id: ObjectId,
    db: Arc<MongoDB>,
    offset: u64,
    data: Vec<u8>,
    map: &mut HashMap<ObjectId, LocalChunk>,
) -> Result<MofuAttribute, nfsstat3> {
    let e = match map.entry(object_id) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => e.insert(LocalChunk::new(object_id, db, 256).await?),
    };
    assert_eq!(e.id, object_id);
    e.append(offset, data).await
}

async fn read(
    object_id: ObjectId,
    db: Arc<MongoDB>,
    offset: u64,
    size: u32,
    map: &mut HashMap<ObjectId, LocalChunk>,
) -> Result<(Vec<u8>, bool), nfsstat3> {
    let e = match map.entry(object_id) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => e.insert(LocalChunk::new(object_id, db, 256).await?),
    };
    assert_eq!(e.id, object_id);
    e.read(offset, size).await
}

async fn get_attr(
    object_id: ObjectId,
    db: Arc<MongoDB>,
    map: &mut HashMap<ObjectId, LocalChunk>,
) -> Result<MofuAttribute, nfsstat3> {
    let e = match map.entry(object_id) {
        Entry::Occupied(e) => e.into_mut(),
        Entry::Vacant(e) => e.insert(LocalChunk::new(object_id, db, 256).await?),
    };
    assert_eq!(e.id, object_id);
    e.attr_commit().await
}
