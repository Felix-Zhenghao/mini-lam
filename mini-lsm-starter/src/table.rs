#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Error, Ok, Result};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        // offset: u32, key_len & value_len: u16
        let mut estimate_size = 0;
        for blockmeta in block_meta {
            estimate_size += std::mem::size_of::<u32>() + 2 * std::mem::size_of::<u16>();
            estimate_size += blockmeta.first_key.len() + blockmeta.last_key.len();
        }
        buf.reserve(estimate_size);
        for blockmeta in block_meta {
            buf.put_u32(blockmeta.offset as u32);
            buf.put_u16(blockmeta.first_key.len() as u16);
            buf.put_u16(blockmeta.last_key.len() as u16);
            buf.put_slice(blockmeta.first_key.raw_ref());
            buf.put_slice(blockmeta.last_key.raw_ref())
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut result: Vec<BlockMeta> = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32();
            let first_key_len = buf.get_u16() as usize;
            let last_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            let last_key = buf.copy_to_bytes(last_key_len);
            result.push(BlockMeta {
                offset: offset as usize,
                first_key: KeyBytes::from_bytes(first_key),
                last_key: KeyBytes::from_bytes(last_key),
            })
        }
        result
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        assert!(file.size() != 0, "Wrong: Read SsTable from an empty file!");

        let meta_block_offset = file.read(file.size() - 4, 4).unwrap().as_slice().get_u32();
        let block_meta = BlockMeta::decode_block_meta(
            file.read(
                meta_block_offset as u64,
                file.size() - 4 - meta_block_offset as u64,
            )?
            .as_slice(),
        );

        let first_key = block_meta[0].first_key.clone();
        let last_key = block_meta.last().unwrap().last_key.clone();

        Ok(SsTable {
            file: file,
            block_meta: block_meta,
            block_meta_offset: meta_block_offset as usize,
            id: id,
            block_cache: block_cache,
            first_key: first_key,
            last_key: last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        assert!(
            block_idx >= self.num_of_blocks(),
            "block_idx out of index when read_block"
        );

        let block_meta = self.block_meta[block_idx].clone();
        let block_offset = block_meta.offset as u64;
        let block_offset_end = {
            if block_idx == self.num_of_blocks() - 1 {
                self.block_meta_offset
            } else {
                self.block_meta[block_idx + 1].offset
            }
        } as u64;

        let block_data = self
            .file
            .read(block_offset, block_offset_end - block_offset)?;
        Ok(Arc::new(Block::decode(block_data.as_slice())))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        let mut low = 0;
        let mut high = self.block_meta.len();

        while low < high {
            let mid = low + (high - low) / 2;
            match key.cmp(&self.block_meta[mid].first_key.as_key_slice()) {
                std::cmp::Ordering::Equal => return mid,
                std::cmp::Ordering::Greater => low = mid + 1,
                std::cmp::Ordering::Less => high = mid,
            }
        }
        low
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }
}
