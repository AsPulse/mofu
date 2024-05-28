use mongodb::bson::oid::ObjectId;
use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MofuChunk {
    #[serde(skip_serializing)]
    pub _id: Option<ObjectId>,
    pub file: ObjectId,
    pub index: u32,
    pub payload: Vec<u8>,
    pub timestamp: DateTime,
}
