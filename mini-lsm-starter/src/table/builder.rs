#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{io::Read, path::Path};

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{Block, BlockBuilder},
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        SsTableBuilder {
            builder: BlockBuilder::new(block_size),
            block_size: block_size,
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        assert!(
            !key.is_empty(),
            "key to be put in SsTable shouldn't be empty"
        );

        // if Block is full, return the block and initialize the builder
        if let false = self.builder.add(key, value) {
            // replace builder
            let new_builder = BlockBuilder::new(self.block_size);
            let old_builder = std::mem::replace(&mut self.builder, new_builder);

            // get block from the old_builder
            let finalized_block = old_builder.build();
            let f_block_first_key = finalized_block.get_first_key();
            let f_block_last_key = finalized_block.get_last_key();

            // get meta of newly-built block
            let finalized_block_meta = BlockMeta {
                offset: self.data.len(),
                first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(f_block_first_key)),
                last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(f_block_last_key)),
            };

            // update field of SsTableBuilder;
            self.data.extend(finalized_block.encode());
            self.meta.push(finalized_block_meta);
            if self.meta.len() == 1 {
                // the first block, update the first key
                self.first_key = f_block_first_key.to_owned();
            }
            let _ = self.builder.add(key, value); // now new builder has be initialized, call add again.
        }
        self.last_key = key.raw_ref().to_owned();
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // build the last block
        let last_block = self.builder.build();
        let last_block_first_key = last_block.get_first_key();
        let last_block_last_key = last_block.get_last_key();
        let last_block_meta = BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(last_block_first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(last_block_last_key)),
        };
        self.data.extend(last_block.encode());
        self.meta.push(last_block_meta);

        // assemble data to be store in file
        let block_meta_offset = self.data.len() as u32;
        let mut all_data = self.data.clone();
        BlockMeta::encode_block_meta(self.meta.clone().as_slice(), &mut all_data);
        all_data.put_u32(block_meta_offset);

        // create file object and write data into file
        let file = FileObject::create(path.as_ref(), all_data)?;

        Ok(SsTable {
            file: file,
            block_meta: self.meta,
            block_meta_offset: block_meta_offset as usize,
            id: id,
            block_cache: block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.as_slice())),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.last_key.as_slice())),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
