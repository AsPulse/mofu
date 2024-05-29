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
                let need_flush = chunk
                    .update
                    .fragment
                    .iter()
                    .filter_map(|(usize, by)| {
                        if by.last_updated.elapsed() > Duration::from_secs(1) {
                            Some(*usize)
                        } else {
                            None
                        }
                    })
                    .collect_vec();
                for i in need_flush {
                    info!("Flushing chunk {:?}", i);
                    chunk.commit(i).await.unwrap_or_else(|e| {
                        error!("Failed to flush chunk: {:?}", e);
                    });
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                msg = rx.recv() => {
                    match msg {
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
