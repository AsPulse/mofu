use std::cmp::min;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use itertools::Itertools;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{self, doc, Bson, DateTime};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument, UpdateOptions};
use nfsserve::nfs::nfsstat3;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{error, info, instrument, warn};

use crate::db::attribute::MofuPayload;
use crate::db::util::{to_bson_and_err, u64_to_bson_and_err};

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

const KB_TO_BYTES: usize = 1024;

pub(crate) struct LocalChunk {
    pub(crate) id: ObjectId,
    pub(crate) db: Arc<MongoDB>,
    attr: MofuAttribute,
    last_attr: Instant,
    pub(crate) size_override: Option<u64>,
    pub(crate) update: LocalChunkFragment,
}

#[derive(Debug, Error)]
pub(crate) enum LocalChunkCreateError {
    #[error("given id is a directory")]
    IsDirectory(MofuAttribute),
    #[error("failed to create new chunk")]
    Error(nfsstat3),
}
impl From<nfsstat3> for LocalChunkCreateError {
    fn from(e: nfsstat3) -> Self {
        LocalChunkCreateError::Error(e)
    }
}

impl From<LocalChunkCreateError> for nfsstat3 {
    fn from(e: LocalChunkCreateError) -> Self {
        match e {
            LocalChunkCreateError::IsDirectory(_) => {
                warn!("given id is a directory");
                nfsstat3::NFS3ERR_ISDIR
            }
            LocalChunkCreateError::Error(e) => e,
        }
    }
}

impl LocalChunk {
    #[instrument(name = "localchunk/new", skip(db))]
    pub(crate) async fn new(
        id: ObjectId,
        db: Arc<MongoDB>,
        default_chunk_size_kb: usize,
    ) -> Result<Self, LocalChunkCreateError> {
        // TODO: check it is not a directory
        let mut attr = db
            .attributes
            .find_one(doc! { "_id": id }, None)
            .await
            .map_err(|e| {
                error!("failed: {}", e);
                nfsstat3::NFS3ERR_IO
            })?
            .ok_or_else(|| {
                error!("failed: not found");
                nfsstat3::NFS3ERR_NOENT
            })?;
        if attr.is_dir {
            return Err(LocalChunkCreateError::IsDirectory(attr));
        }

        if attr.payload.is_none() {
            warn!("chunk payload is not found, creating new one");
            let result = db.attributes.find_one_and_update(
                doc! {
                    "_id": id
                },
                doc! {
                    "$set": {
                        "payload":  to_bson_and_err(&MofuPayload::InternalChunk { buffer_kb: default_chunk_size_kb })?
                    }
                },
                FindOneAndUpdateOptions::builder().return_document(ReturnDocument::After).build(),
            ).await.map_err(|e| {
                error!("failed: {}", e);
                nfsstat3::NFS3ERR_IO
            })?.ok_or(nfsstat3::NFS3ERR_IO)?;
            attr = result;
        }
        let Some(MofuPayload::InternalChunk {
            buffer_kb: chunk_size_kb,
        }) = attr.payload
        else {
            panic!("unexpected payload type");
        };

        Ok(Self {
            id,
            db,
            attr,
            update: LocalChunkFragment {
                chunk_size_kb,
                fragment: BTreeMap::new(),
                commit: BTreeMap::new(),
            },
            last_attr: Instant::now(),
            size_override: None,
        })
    }

    pub(crate) async fn read(
        &mut self,
        offset: u64,
        size: u32,
    ) -> Result<(Vec<u8>, bool), nfsstat3> {
        let mut data: Vec<u8> = Vec::with_capacity(size.try_into().unwrap());

        self.attr = self
            .db
            .attributes
            .find_one(doc! { "_id": self.id }, None)
            .await
            .map_err(|e| {
                error!("failed: {}", e);
                nfsstat3::NFS3ERR_IO
            })?
            .ok_or_else(|| {
                error!("failed: not found");
                nfsstat3::NFS3ERR_NOENT
            })?;

        let size_e = min(size as u64, self.attr.size.saturating_sub(offset));
        let eof = offset + size as u64 >= self.attr.size;

        for (index, dst_start, dst_end, _, _) in split_by_size(
            offset,
            size_e as u64,
            self.update.chunk_size_kb * KB_TO_BYTES,
        ) {
            let frag = self.update.fragment.get(&index);
            if let Some(o) = frag {
                if let Some(rx) = self.update.commit.remove(&index) {
                    if rx.await.is_err() {
                        warn!("previous-commit is failed.");
                    }
                }
                if o.vec.len() == 1
                    && o.vec[0].start == 0
                    && o.vec[0].end == self.update.chunk_size_kb * KB_TO_BYTES
                {
                    data.extend(o.vec[0].data[dst_start..dst_end].iter());
                    continue;
                }
            }

            let chunk = self
                .db
                .chunks
                .find_one(
                    doc! {
                        "file": self.id,
                        "index": index as u32
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    error!("failed: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?
                .map(|x| match x.payload {
                    Bson::Binary(b) => b.bytes,
                    _ => {
                        warn!("unexpected payload type");
                        Vec::new()
                    }
                })
                .unwrap_or_else(|| vec![0; self.update.chunk_size_kb * KB_TO_BYTES]);

            match frag {
                Some(o) => {
                    data.extend(o.overlay(chunk)[dst_start..dst_end].iter());
                }
                None => {
                    data.extend(chunk[dst_start..dst_end].iter());
                }
            }
        }
        Ok((data, eof))
    }

    pub(crate) async fn append(
        &mut self,
        offset: u64,
        data: Vec<u8>,
    ) -> Result<MofuAttribute, nfsstat3> {
        self.update.append(offset, data);
        let new_size = self
            .update
            .fragment
            .last_key_value()
            .map(|(size, last)| {
                let last_len = last.vec.iter().map(|x| x.end).max().unwrap_or(0);
                *size as u64 * self.update.chunk_size_kb as u64 * KB_TO_BYTES as u64
                    + last_len as u64
            })
            .unwrap_or(0);

        if self.attr.size < new_size {
            self.size_override = Some(new_size);
        }
        Ok(self.attr.clone())
    }

    #[instrument(name = "localchunk/commit", skip(self), fields(id = self.id.to_hex()))]
    pub(crate) async fn commit_rest(&mut self) -> Result<(), nfsstat3> {
        let need_flush = self
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
        let len = need_flush.len();
        let mut task = Vec::with_capacity(len);
        for i in &need_flush {
            let Some(o) = self.update.fragment.remove(i) else {
                break;
            };
            let (tx, rx) = oneshot::channel::<()>();
            let rx = self.update.commit.insert(*i, rx);
            task.push((*i, o, tx, rx));
        }
        if len > 0 {
            self.attr_commit().await?;
            info!("{} chunks flush requested.", len);
            let (id, db, chunk_size_kb) = (self.id, self.db.clone(), self.update.chunk_size_kb);
            tokio::spawn(async move {
                for (i, o, tx, rx) in task {
                    if let Err(e) = commit_task(id, db.clone(), chunk_size_kb, i, o, rx, tx).await {
                        error!("failed flushing chunk {}: {:?}", i, e);
                    }
                }
                info!("{} chunks flushed.", len);
            });
        }
        Ok(())
    }

    pub(crate) async fn attr_commit(&mut self) -> Result<MofuAttribute, nfsstat3> {
        self.attr = match self.size_override.take() {
            Some(size) => self
                .db
                .attributes
                .find_one_and_update(
                    doc! {
                        "_id": self.id
                    },
                    doc! {
                        "$set": {
                            "size": u64_to_bson_and_err(size)?
                        }
                    },
                    FindOneAndUpdateOptions::builder()
                        .return_document(ReturnDocument::After)
                        .build(),
                )
                .await
                .map_err(|e| {
                    error!("failed: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?
                .ok_or_else(|| {
                    error!("failed: not found");
                    nfsstat3::NFS3ERR_NOENT
                })?,
            None => {
                if self.last_attr.elapsed() <= Duration::from_secs(3) {
                    self.attr.clone()
                } else {
                    self.db
                        .attributes
                        .find_one(doc! { "_id": self.id }, None)
                        .await
                        .map_err(|e| {
                            error!("failed: {}", e);
                            nfsstat3::NFS3ERR_IO
                        })?
                        .ok_or_else(|| {
                            error!("failed: not found");
                            nfsstat3::NFS3ERR_NOENT
                        })?
                }
            }
        };
        self.last_attr = Instant::now();
        Ok(self.attr.clone())
    }
}

async fn commit_task(
    id: ObjectId,
    db: Arc<MongoDB>,
    chunk_size_kb: usize,
    index: usize,
    o: LocalChunkFragmentBy,
    rx: Option<oneshot::Receiver<()>>,
    tx: oneshot::Sender<()>,
) -> Result<(), nfsstat3> {
    if let Some(rx) = rx {
        if rx.await.is_err() {
            warn!("awaiting previous-commit is failed.");
        }
    }
    let x =
        if o.vec.len() == 1 && o.vec[0].start == 0 && o.vec[0].end == chunk_size_kb * KB_TO_BYTES {
            Bson::Binary(bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: o.vec[0].data.clone(),
            })
        } else {
            let data = db
                .chunks
                .find_one(
                    doc! {
                        "file": id,
                        "index": index as u32
                    },
                    None,
                )
                .await
                .map_err(|e| {
                    error!("failed: {}", e);
                    nfsstat3::NFS3ERR_IO
                })?
                .map(|x| match x.payload {
                    Bson::Binary(b) => b.bytes,
                    _ => {
                        warn!("unexpected payload type");
                        Vec::new()
                    }
                })
                .unwrap_or(Vec::new());

            Bson::Binary(bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: o.overlay(data),
            })
        };

    db.chunks
        .update_one(
            doc! {
                "file": id,
                "index": index as u32
            },
            doc! {
                "$set": {
                    "payload": x,
                    "timestamp": DateTime::now()
                }
            },
            UpdateOptions::builder().upsert(true).build(),
        )
        .await
        .map_err(|e| {
            error!("failed: {}", e);
            nfsstat3::NFS3ERR_IO
        })?;
    if tx.send(()).is_err() {
        warn!("previous-commit receiver is dropped.");
    }
    Ok(())
}

pub(crate) struct LocalChunkFragment {
    chunk_size_kb: usize,
    pub(crate) fragment: BTreeMap<usize, LocalChunkFragmentBy>,
    pub(crate) commit: BTreeMap<usize, oneshot::Receiver<()>>,
}

pub(crate) struct LocalChunkFragmentBy {
    vec: Vec<RenderedLocalChunkFragment>,
    pub(crate) last_updated: Instant,
}

impl LocalChunkFragment {
    fn append(&mut self, offset: u64, data: Vec<u8>) {
        for (index, dst_start, dst_end, src_start, src_end) in
            split_by_size(offset, data.len() as u64, self.chunk_size_kb * KB_TO_BYTES)
        {
            let chunk = self.fragment.entry(index).or_insert(LocalChunkFragmentBy {
                vec: Vec::new(),
                last_updated: Instant::now(),
            });
            chunk.vec.push(RenderedLocalChunkFragment {
                start: dst_start,
                end: dst_end,
                data: data[src_start..src_end].to_vec(),
            });
            chunk.render_local_chunk();
            chunk.last_updated = Instant::now();
        }
    }
}

#[derive(Debug, PartialEq, Clone)]
struct RenderedLocalChunkFragment {
    start: usize,
    end: usize,
    data: Vec<u8>,
}
impl LocalChunkFragmentBy {
    fn overlay(&self, mut original: Vec<u8>) -> Vec<u8> {
        if self.vec.is_empty() {
            return original;
        }
        let need_size = self.vec.iter().map(|x| x.end).max().unwrap();
        if original.len() < need_size {
            original.resize(need_size, 0);
        }
        for v in &self.vec {
            original[v.start..v.end].copy_from_slice(&v.data);
        }
        original
    }

    fn render_local_chunk(&mut self) {
        self.vec = self.vec.clone().into_iter().fold(Vec::new(), |acc, v| {
            if acc.is_empty() {
                vec![v.clone()]
            } else {
                let mut ret = acc;
                if let Some((i, _)) = ret
                    .iter()
                    .find_position(|x| v.start <= x.start && x.end <= v.end)
                {
                    ret[i] = v;
                } else if let Some((i, _)) = ret
                    .iter()
                    .find_position(|x| x.start <= v.start && v.end <= x.end)
                {
                    let wide = &ret[i];
                    let narrow = v;
                    let mut new: Vec<u8> = Vec::with_capacity(wide.data.len());
                    new.extend(wide.data[0..(narrow.start - wide.start)].iter());
                    new.extend(narrow.data.iter());
                    new.extend(wide.data[(narrow.end - wide.start)..].iter());
                    ret[i].data = new;
                } else if let Some((i, _)) = ret
                    .iter()
                    .find_position(|x| x.start <= v.start && v.start <= x.end)
                {
                    let left = &ret[i];
                    let right = v;
                    let mut new: Vec<u8> = Vec::with_capacity(left.data.len());
                    new.extend(left.data[0..(right.start - left.start)].iter());
                    new.extend(right.data.iter());
                    ret[i].data = new;
                    ret[i].end = right.end;
                } else if let Some((i, _)) = ret
                    .iter()
                    .find_position(|x| x.start <= v.end && v.end <= x.end)
                {
                    let right = &ret[i];
                    let left = v;
                    let mut new: Vec<u8> = Vec::with_capacity(right.data.len());
                    new.extend(left.data.iter());
                    new.extend(right.data[(left.end - right.start)..].iter());
                    ret[i].data = new;
                    ret[i].start = left.start;
                } else {
                    ret.push(v);
                }
                ret
            }
        })
    }
}

fn split_by_size(
    offset: u64,
    size: u64,
    chunk_size: usize,
) -> Vec<(usize, usize, usize, usize, usize)> {
    let mut chunks = Vec::new();
    let mut src_pointer = 0;
    let mut dst_pointer = (
        usize::try_from(offset / chunk_size as u64).unwrap(),
        usize::try_from(offset % chunk_size as u64).unwrap(),
    );

    while src_pointer < size {
        let src_remaining = size - src_pointer;
        let dst_remaining = chunk_size - dst_pointer.1;
        let step = min(src_remaining as usize, dst_remaining);
        let new_src_pointer = src_pointer + step as u64;
        let new_dst_pointer = dst_pointer.1 + step;
        chunks.push((
            dst_pointer.0,
            dst_pointer.1,
            new_dst_pointer as usize,
            src_pointer as usize,
            new_src_pointer as usize,
        ));
        src_pointer = new_src_pointer;
        dst_pointer = (
            dst_pointer.0 + if new_dst_pointer >= chunk_size { 1 } else { 0 },
            new_dst_pointer % chunk_size,
        );
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_by_size() {
        assert_eq!(split_by_size(0, 10, 10), [(0, 0, 10, 0, 10)]);
        assert_eq!(split_by_size(0, 10, 5), [(0, 0, 5, 0, 5), (1, 0, 5, 5, 10)]);
        assert_eq!(
            split_by_size(0, 10, 3),
            [
                (0, 0, 3, 0, 3),
                (1, 0, 3, 3, 6),
                (2, 0, 3, 6, 9),
                (3, 0, 1, 9, 10)
            ]
        );
        assert_eq!(
            split_by_size(6, 10, 4),
            [(1, 2, 4, 0, 2), (2, 0, 4, 2, 6), (3, 0, 4, 6, 10)]
        );
        assert_eq!(split_by_size(3, 10, 100), [(0, 3, 13, 0, 10)]);
        assert_eq!(split_by_size(103, 10, 100), [(1, 3, 13, 0, 10)]);
    }

    #[test]
    fn test_render_local_chunk() {
        let mut f = LocalChunkFragmentBy {
            vec: vec![
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 3,
                    data: vec![0, 1, 2],
                },
                RenderedLocalChunkFragment {
                    start: 4,
                    end: 7,
                    data: vec![5, 6, 7],
                },
            ],
            last_updated: Instant::now(),
        };
        f.render_local_chunk();
        assert_eq!(
            f.vec,
            vec![
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 3,
                    data: vec![0, 1, 2],
                },
                RenderedLocalChunkFragment {
                    start: 4,
                    end: 7,
                    data: vec![5, 6, 7],
                },
            ]
        );
        let mut f = LocalChunkFragmentBy {
            vec: vec![
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 10,
                    data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                },
                RenderedLocalChunkFragment {
                    start: 10,
                    end: 20,
                    data: vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
                },
            ],
            last_updated: Instant::now(),
        };
        f.render_local_chunk();
        assert_eq!(
            f.vec,
            vec![RenderedLocalChunkFragment {
                start: 0,
                end: 20,
                data: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
            },]
        );
        let mut f = LocalChunkFragmentBy {
            vec: vec![
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 3,
                    data: vec![0, 1, 2],
                },
                RenderedLocalChunkFragment {
                    start: 2,
                    end: 5,
                    data: vec![5, 6, 7],
                },
            ],
            last_updated: Instant::now(),
        };
        f.render_local_chunk();
        assert_eq!(
            f.vec,
            vec![RenderedLocalChunkFragment {
                start: 0,
                end: 5,
                data: vec![0, 1, 5, 6, 7]
            },]
        );
        let no_change = vec![
            RenderedLocalChunkFragment {
                start: 0,
                end: 3,
                data: vec![0, 1, 2],
            },
            RenderedLocalChunkFragment {
                start: 4,
                end: 7,
                data: vec![5, 6, 7],
            },
        ];
        let mut f = LocalChunkFragmentBy {
            vec: no_change.clone(),
            last_updated: Instant::now(),
        };
        f.render_local_chunk();
        assert_eq!(f.vec, no_change);

        let mut f = LocalChunkFragmentBy {
            vec: vec![
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 3,
                    data: vec![0, 1, 2],
                },
                RenderedLocalChunkFragment {
                    start: 1,
                    end: 2,
                    data: vec![5],
                },
            ],
            last_updated: Instant::now(),
        };
        f.render_local_chunk();

        assert_eq!(
            f.vec,
            vec![RenderedLocalChunkFragment {
                start: 0,
                end: 3,
                data: vec![0, 5, 2],
            }]
        );
        let mut f = LocalChunkFragmentBy {
            vec: vec![
                RenderedLocalChunkFragment {
                    start: 1,
                    end: 2,
                    data: vec![5],
                },
                RenderedLocalChunkFragment {
                    start: 0,
                    end: 3,
                    data: vec![0, 1, 2],
                },
            ],
            last_updated: Instant::now(),
        };
        f.render_local_chunk();
        assert_eq!(
            f.vec,
            vec![RenderedLocalChunkFragment {
                start: 0,
                end: 3,
                data: vec![0, 1, 2],
            }]
        );
    }

    #[test]
    fn test_overlay() {
        let a = LocalChunkFragmentBy {
            vec: vec![RenderedLocalChunkFragment {
                start: 3,
                end: 5,
                data: vec![6, 7],
            }],
            last_updated: Instant::now(),
        };
        assert_eq!(
            a.overlay(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            vec![0, 1, 2, 6, 7, 5, 6, 7, 8, 9]
        );
        assert_eq!(a.overlay(vec![1, 5]), vec![1, 5, 0, 6, 7]);
    }
}
