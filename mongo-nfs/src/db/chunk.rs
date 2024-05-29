use std::cmp::min;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;
use mongodb::bson::oid::ObjectId;
use mongodb::bson::spec::BinarySubtype;
use mongodb::bson::{self, doc, Bson, DateTime};
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument, UpdateOptions};
use nfsserve::nfs::nfsstat3;
use serde::{Deserialize, Serialize};
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
    pub(crate) update: LocalChunkFragment,
}

impl LocalChunk {
    #[instrument(name = "localchunk/new", skip(db))]
    pub(crate) async fn new(
        id: ObjectId,
        db: Arc<MongoDB>,
        default_chunk_size_kb: usize,
    ) -> Result<Self, nfsstat3> {
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
                nfsstat3::NFS3ERR_IO
            })?;

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
            },
        })
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
            info!("updating size {} -> {}", self.attr.size, new_size);
            self.attr = self
                .db
                .attributes
                .find_one_and_update(
                    doc! { "_id": self.id },
                    doc! { "$set": { "size": u64_to_bson_and_err(new_size)? } },
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
                    nfsstat3::NFS3ERR_IO
                })?;
        }
        Ok(self.attr.clone())
    }

    pub(crate) async fn commit(&mut self, index: usize) -> Result<(), nfsstat3> {
        let Some(o) = self.update.fragment.remove(&index) else {
            return Ok(());
        };

        let x = if o.vec.len() == 1
            && o.vec[0].start == 0
            && o.vec[0].end == self.update.chunk_size_kb * KB_TO_BYTES
        {
            Bson::Binary(bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: o.vec[0].data.clone(),
            })
        } else {
            let data = self
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
                .unwrap_or(Vec::new());

            Bson::Binary(bson::Binary {
                subtype: BinarySubtype::Generic,
                bytes: o.overlay(data),
            })
        };

        self.db
            .chunks
            .update_one(
                doc! {
                    "file": self.id,
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

        Ok(())
    }
}

pub(crate) struct LocalChunkFragment {
    chunk_size_kb: usize,
    pub(crate) fragment: BTreeMap<usize, LocalChunkFragmentBy>,
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
