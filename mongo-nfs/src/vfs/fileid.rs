use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use bimap::BiHashMap;
use mongodb::bson::oid::ObjectId;

use super::mountpoint::MountpointId;

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum FileId {
    Root,
    FileSystemRoot(MountpointId),
    ObjectId(MountpointId, ObjectId),
}

pub struct FileIdMap {
    lowest: Arc<AtomicU64>,
    map: Arc<RwLock<BiHashMap<FileId, u64>>>,
}

impl FileIdMap {
    pub fn new() -> Self {
        Self {
            lowest: Arc::new(AtomicU64::new(1)),
            map: Arc::new(RwLock::new(BiHashMap::new())),
        }
    }

    pub fn insert(&self, file_id: FileId) -> u64 {
        let mut map = self.map.write().unwrap();
        let id = self
            .lowest
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        map.insert(file_id, id);
        id
    }

    pub fn get_fileid_or_insert(&self, file_id: FileId) -> u64 {
        let map = self.map.read().unwrap();
        if let Some(id) = map.get_by_left(&file_id) {
            *id
        } else {
            drop(map);
            self.insert(file_id)
        }
    }

    pub fn get_fileid(&self, id: u64) -> Option<FileId> {
        let map = self.map.read().unwrap();
        map.get_by_right(&id).cloned()
    }

    pub fn get_u64(&self, file_id: &FileId) -> Option<u64> {
        let map = self.map.read().unwrap();
        map.get_by_left(file_id).cloned()
    }
}
