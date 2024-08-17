#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::{Block, SIZEOF_U16};

pub(crate) const SIZEOF_U8: usize = std::mem::size_of::<u8>();

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        BlockBuilder {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size: block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key should not be empty");

        let cur_len = self.data.len() + SIZEOF_U16 * self.offsets.len() + SIZEOF_U16;
        let add_len = key.len() + value.len() + 3 * SIZEOF_U16;
        match cur_len + add_len > self.block_size && !self.is_empty() {
            true => false,
            false => {
                self.offsets.push(self.data.len() as u16);
                self.data.put_u16(key.len() as u16);
                self.data.put(key.raw_ref());
                self.data.put_u16(value.len() as u16);
                self.data.put(value);
                true
            }
        }
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }
}
