use attribute::MofuAttribute;
use mongodb::bson::doc;
use mongodb::options::IndexOptions;
use mongodb::IndexModel;
use thiserror::Error;
use tracing::{info, instrument};

use self::bucket::MofuBucket;
use self::chunk::MofuChunk;

pub mod attribute;
pub mod bucket;
pub mod chunk;
pub mod time;

#[derive(Clone)]
pub(crate) struct MongoDB {
    source: String,
    pub client: mongodb::Client,
    pub db: mongodb::Database,

    pub attributes: mongodb::Collection<MofuAttribute>,
    pub buckets: mongodb::Collection<MofuBucket>,
    pub chunks: mongodb::Collection<MofuChunk>,
}

#[derive(Error, Debug)]
pub enum MongoDBError {
    #[error("failed to connect to MongoDB ({0})")]
    ConnectionFailed(String, mongodb::error::Error),

    #[error("an error occured during creating index: {1} ({0})")]
    IndexCreationFailed(String, mongodb::error::Error),
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
            chunks: db.collection("chunks"),
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

        mongo
            .attributes
            .create_index(
                IndexModel::builder()
                    .keys(doc! {
                        "parent": 1,
                        "name": 1
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .map_err(|e| MongoDBError::IndexCreationFailed(source.clone(), e))?;

        info!("created index for attributes collection");

        mongo
            .chunks
            .create_index(
                IndexModel::builder()
                    .keys(doc! {
                        "file": 1,
                        "index": 1
                    })
                    .options(IndexOptions::builder().unique(true).build())
                    .build(),
                None,
            )
            .await
            .map_err(|e| MongoDBError::IndexCreationFailed(source.clone(), e))?;

        info!("created index for chunks collection");

        Ok(mongo)
    }
}
