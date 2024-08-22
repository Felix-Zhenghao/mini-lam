#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

use std::cmp;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        Ok(TwoMergeIterator { a, b })
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.a.is_valid() && self.b.is_valid() {
            cmp::min(self.a.key(), self.b.key())
        } else if self.a.is_valid() {
            // only a is valid
            self.a.key()
        } else {
            // only b is valid
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.a.is_valid() && self.b.is_valid() {
            // get value from the one with smaller key
            if self.a.key() < self.b.key() {
                self.a.value()
            } else if self.a.key() > self.b.key() {
                self.b.value()
            } else {
                self.a.value()
            }
        } else if self.a.is_valid() {
            // only a is valid
            self.a.value()
        } else {
            // only b is valid
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.a.is_valid() && self.b.is_valid() {
            // get value from the one with smaller key
            if self.a.key() < self.b.key() {
                self.a.next()?;
            } else if self.a.key() > self.b.key() {
                self.b.next()?;
            } else {
                self.a.next()?;
                self.b.next()?;
            }
        } else if self.a.is_valid() {
            // only a is valid
            self.a.next()?;
        } else if self.b.is_valid() {
            // only b is valid
            self.b.next()?;
        }
        let a_valid = self.a.is_valid();
        let b_valid = self.b.is_valid();
        let two_valid = self.is_valid();
        Ok(())
    }
}
