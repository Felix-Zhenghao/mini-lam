#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::{self, PeekMut};
use std::collections::BinaryHeap;

use anyhow::{Ok, Result};
use bytes::Bytes;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// First, compare current key of the two iterators
// Then, compare the id of the index of the heap wrapper
impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // if no sub-iter, nothing to merge
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }

        // if all sub-iters are invalid (no item to iter)
        if iters.iter().all(|x| !x.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: BinaryHeap::new(),
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        let mut heap = BinaryHeap::new();
        for (index, sub_iter) in iters.into_iter().enumerate() {
            if sub_iter.is_valid() {
                heap.push(HeapWrapper(index, sub_iter));
            }
        }

        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|x| x.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // remember, each skipmap is sorted and the big heap of skipmap is also sorted
        // 1. deal with same key -> only get the latest entry
        let current = self.current.as_mut().unwrap();
        while let Some(mut inner) = self.iters.peek_mut() {
            debug_assert!(inner.1.key() >= current.1.key(), "heap invariant violated");

            // if some old entry exist, call `next` to all of the iters with old entry
            if inner.1.key() == current.1.key() {
                if let e @ Err(_) = inner.1.next() {
                    PeekMut::pop(inner);
                    return e;
                }
                if !inner.1.is_valid() {
                    PeekMut::pop(inner);
                }
            } else {
                break;
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(inner) = self.iters.pop() {
                *current = inner;
            }
            return Ok(());
        }

        if let Some(mut inner) = self.iters.peek_mut() {
            if *current < *inner {
                std::mem::swap(&mut *inner, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        let mut result = 0;
        if let Some(iter) = self.current.as_ref() {
            result += iter.1.num_active_iterators();
        }
        self.iters
            .iter()
            .for_each(|x| result += x.1.num_active_iterators());
        result
    }
}
