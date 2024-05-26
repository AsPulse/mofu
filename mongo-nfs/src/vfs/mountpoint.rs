use std::collections::BTreeMap;
use std::sync::Arc;

use bimap::BiHashMap;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::{doc, DateTime};
use thiserror::Error;
use tracing::{info, instrument};

use crate::config::Config;
use crate::db::bucket::MofuBucket;
use crate::db::MongoDB;
pub type MountpointId = u8;

pub(crate) struct MountpointMap {
    pub id_map: BiHashMap<String, MountpointId>,
    pub map: BTreeMap<MountpointId, Mountpoint>,
}

pub(crate) struct Mountpoint {
    pub db: MongoDB,
    pub bucket: ObjectId,
}

#[derive(Error, Debug)]
pub enum MountPointInitializeError {
    #[error("the number of mountpoints exceeds the limit (max 255)")]
    TooManyMountpoints,
    #[error("error occurred in MongoDB: {0}")]
    MongoDBError(#[from] mongodb::error::Error),
}

impl MountpointMap {
    pub async fn new(
        config: &Config,
        db: &Arc<BTreeMap<String, MongoDB>>,
    ) -> Result<Self, MountPointInitializeError> {
        let mut id_map = BiHashMap::new();
        let mut map = BTreeMap::new();
        for (i, p) in config.mountpoints.iter().enumerate() {
            let db = db.get(&p.source).unwrap();
            let mountpoint = Mountpoint::new(db.clone(), p.bucket.clone()).await?;

            let id = MountpointId::try_from(i as u8)
                .map_err(|_| MountPointInitializeError::TooManyMountpoints)?;

            id_map.insert(p.path.clone(), id);
            map.insert(id, mountpoint);
        }
        Ok(Self { map, id_map })
    }
}
impl Mountpoint {
    #[instrument(name = "mountpoint/new", skip(db))]
    pub async fn new(db: MongoDB, bucket: String) -> Result<Self, MountPointInitializeError> {
        let docs = db
            .buckets
            .find_one(
                doc! {
                    "name": &bucket
                },
                None,
            )
            .await?;
        let bucket = match docs {
            Some(doc) => {
                // This doc is fetched from MongoDB so it must have an _id field
                let id = doc._id.unwrap();
                info!("found bucket: {:?}", id);
                id
            }
            None => {
                info!("creating bucket...");
                let id = db
                    .buckets
                    .insert_one(
                        MofuBucket {
                            _id: None,
                            name: bucket.clone(),
                            created_at: DateTime::now(),
                        },
                        None,
                    )
                    .await?
                    .inserted_id
                    .as_object_id()
                    .unwrap();
                info!("created bucket: {:?}", id);
                id
            }
        };
        Ok(Self { db, bucket })
    }
}

impl MountpointMap {
    pub fn get(&self, id: MountpointId) -> &Mountpoint {
        self.map.get(&id).unwrap()
    }

    pub fn get_id(&self, source: &str) -> Option<MountpointId> {
        self.id_map.get_by_left(source).copied()
    }
}
