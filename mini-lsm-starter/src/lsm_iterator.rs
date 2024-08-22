use anyhow::{bail, Ok, Result};
use bytes::Bytes;

use crate::{
    iterators::two_merge_iterator::TwoMergeIterator,
    iterators::{merge_iterator::MergeIterator, StorageIterator},
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

use std::ops::Bound;

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, end_bound: Bound<&[u8]>) -> Result<Self> {
        let mut iter = Self {
            is_valid: iter.is_valid(),
            inner: iter,
            end_bound: match end_bound {
                Bound::Included(key) => Bound::Included(Bytes::copy_from_slice(key)),
                Bound::Excluded(key) => Bound::Excluded(Bytes::copy_from_slice(key)),
                Bound::Unbounded => Bound::Unbounded,
            },
        };
        iter.move_to_non_delete()?;
        Ok(iter)
    }
}

impl LsmIterator {
    fn move_to_non_delete(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
            if !self.inner.is_valid() {
                self.is_valid = false;
            }
            match self.end_bound.as_ref() {
                Bound::Unbounded => {}
                Bound::Excluded(key) => self.is_valid = self.key() < key.as_ref(),
                Bound::Included(key) => self.is_valid = self.key() <= key.as_ref(),
            }
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        if !self.inner.is_valid() {
            self.is_valid = false;
            return Ok(());
        }

        match self.end_bound.as_ref() {
            Bound::Unbounded => {}
            Bound::Excluded(key) => self.is_valid = self.key() < key.as_ref(),
            Bound::Included(key) => self.is_valid = self.key() <= key.as_ref(),
        }

        self.move_to_non_delete()?;

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a> = I::KeyType<'a> where Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        if self.has_errored || !self.iter.is_valid() {
            panic!("underlying iterator unaccessible")
        }
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        if self.has_errored || !self.iter.is_valid() {
            panic!("underlying iterator unaccessible")
        }
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            bail!("the iterator already has error")
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
