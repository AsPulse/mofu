use attribute::MofuAttribute;
use mongodb::bson::doc;
use thiserror::Error;
use tracing::{info, instrument};

use self::bucket::MofuBucket;

pub mod attribute;
pub mod bucket;
pub mod time;

#[derive(Clone)]
pub(crate) struct MongoDB {
    source: String,
    pub client: mongodb::Client,
    pub db: mongodb::Database,

    pub attributes: mongodb::Collection<MofuAttribute>,
    pub buckets: mongodb::Collection<MofuBucket>,
}

#[derive(Error, Debug)]
pub enum MongoDBError {
    #[error("failed to connect to MongoDB ({0})")]
    ConnectionFailed(String, mongodb::error::Error),
}

impl MongoDB {
    #[instrument(name = "mongodb/connect", skip_all, fields(source = %source))]
    pub async fn new(source: String, uri: &str, db: &str) -> Result<Self, MongoDBError> {
        info!("connecting to MongoDB...");
        let client = mongodb::Client::with_uri_str(uri)
            .await
            .map_err(|e| MongoDBError::ConnectionFailed(source.clone(), e))?;

        let db = client.database(db);
        let mongo = Self {
            source: source.clone(),
            client,
            attributes: db.collection("attributes"),
            buckets: db.collection("buckets"),
            db,
        };
        mongo
            .db
            .run_command(
                doc! {
                    "ping": 1
                },
                None,
            )
            .await
            .map_err(|e| MongoDBError::ConnectionFailed(source.clone(), e))?;

        info!("got ping response from MongoDB");
        Ok(mongo)
    }
}
