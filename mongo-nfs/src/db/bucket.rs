use mongodb::bson::oid::ObjectId;
use mongodb::bson::DateTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MofuBucket {
    #[serde(skip_serializing)]
    pub _id: Option<ObjectId>,
    pub name: String,
    pub created_at: DateTime,
}
