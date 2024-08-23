#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{cmp, sync::Arc};

use anyhow::{Ok, Result};

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        debug_assert!(
            table.block_meta.len() != 0,
            "no block in table to so can't create an table iterator"
        );
        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(0).unwrap());
        let new_table_iter = SsTableIterator {
            table: table,
            blk_iter: blk_iter,
            blk_idx: 0,
        };
        Ok(new_table_iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let refreshed_iter =
            BlockIterator::create_and_seek_to_first(self.table.read_block_cached(0).unwrap());
        self.blk_iter = refreshed_iter;
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        debug_assert!(
            table.block_meta.len() != 0,
            "no block in table to so can't create an table iterator"
        );

        // if key is bigger than any other key, return an invalid iterator
        if key > table.last_key.as_key_slice() {
            return Ok(SsTableIterator {
                table: Arc::clone(&table),
                blk_iter: BlockIterator::create_and_seek_to_key(
                    table.read_block_cached(0).unwrap(),
                    key,
                ),
                blk_idx: table.block_meta.len() - 1,
            });
        }

        let mut new_table_iter = SsTableIterator::create_and_seek_to_first(table)?;
        new_table_iter.seek_to_key(key)?;
        Ok(new_table_iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        if key > self.table.last_key.as_key_slice() {
            self.blk_iter = BlockIterator::create_and_seek_to_key(
                self.table.read_block_cached(0).unwrap(),
                key,
            );
            self.blk_idx = self.table.block_meta.len() - 1;
            return Ok(());
        }

        let mut low: usize = 0;
        let mut high: usize = self.table.block_meta.len();
        let target_id: usize;
        let binary_search = |low: &mut usize, high: &mut usize| -> usize {
            while *low < *high {
                let mid = *low + (*high - *low) / 2;
                match key.cmp(&KeySlice::from_slice(
                    self.table
                        .read_block_cached(mid)
                        .unwrap()
                        .get_last_key()
                        .as_slice(),
                )) {
                    cmp::Ordering::Equal => return mid,
                    cmp::Ordering::Less => *high = mid,
                    cmp::Ordering::Greater => *low = mid + 1,
                }
            }
            *low
        };
        target_id = binary_search(&mut low, &mut high);

        let target_block_iter = BlockIterator::create_and_seek_to_key(
            self.table.read_block_cached(target_id).unwrap(),
            key,
        );
        if !target_block_iter.is_valid() {
            let target_id = target_id + 1;
            let target_block_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(target_id).unwrap(),
            );
        }
        self.blk_iter = target_block_iter;
        self.blk_idx = target_id;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        // if the current block iterator is invalid and no more block in table -> invalid
        self.blk_iter.is_valid() || self.blk_idx < self.table.block_meta.len() - 1
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if (!self.blk_iter.is_valid()) && (self.blk_idx < self.table.block_meta.len() - 1) {
            self.blk_idx += 1;
            let new_block = self.table.read_block_cached(self.blk_idx).unwrap();
            let new_block_iterator = BlockIterator::create_and_seek_to_first(new_block);
            self.blk_iter = new_block_iterator;
        }
        Ok(())
    }
}
