use std::ops::RangeBounds;

use mongodb::bson::spec::BinarySubtype;
use tracing::{error, info, warn};

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
