#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::mem;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use clap::builder::StringValueParser;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::{Block, BlockBuilder};
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::{self, TwoMergeIterator};
use crate::iterators::StorageIterator;
use crate::key::{Key, KeySlice};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::table::{bloom, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // TODO: Understand this.
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        if let Some(handle) = self.compaction_thread.lock().take() {
            handle.join().unwrap();
        }
        if let Some(handle) = self.flush_thread.lock().take() {
            handle.join().unwrap();
        }
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard) // TODO: Understand this.
        };

        // search on the current updating memtable
        if let Some(value) = snapshot.memtable.get(_key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        // search on the memtable vec that has been archived
        for mem_table in snapshot.imm_memtables.iter() {
            if let Some(value) = mem_table.get(_key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        // search in SSTs
        for sst_id in snapshot.l0_sstables.iter() {
            let sst = snapshot.sstables.get(sst_id).unwrap();
            if let Some(bloom) = &sst.bloom {
                if !bloom.may_contain(farmhash::fingerprint32(_key)) {
                    continue;
                }
            }
            let sst_iter = SsTableIterator::create_and_seek_to_key(
                Arc::clone(sst),
                KeySlice::from_slice(_key),
            )
            .unwrap();
            if sst_iter.is_valid() && sst_iter.key() == KeySlice::from_slice(_key) {
                if sst_iter.value().is_empty() {
                    return Ok(None);
                } else {
                    return Ok(Some(Bytes::copy_from_slice(sst_iter.value())));
                }
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let size;

        {
            let guard = self.state.read();
            guard.memtable.put(_key, _value)?;
            size = guard.memtable.approximate_size();
        } // no guard anymore, but size has been archived

        self.try_freeze(size)?;

        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        self.put(_key, &[])
    }

    fn try_freeze(&self, app_size: usize) -> Result<()> {
        if app_size >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();

            let guard = self.state.read();
            if guard.memtable.approximate_size() >= self.options.target_sst_size {
                drop(guard);
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_table = Arc::new(MemTable::create(self.next_sst_id()));
        let old_memtable;

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            old_memtable = std::mem::replace(&mut snapshot.memtable, new_table);
            snapshot.imm_memtables.insert(0, old_memtable.clone());

            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard) // TODO: Understand this.
        };

        // process the block builder from the memtable data
        let memtable_to_flush = snapshot.imm_memtables.last().unwrap();
        let mut sst_builder = SsTableBuilder::new(1024); // configure the block size to 1024
        memtable_to_flush.flush(&mut sst_builder)?;
        let id = self.next_sst_id();
        let path = self.path_of_sst(id);
        let new_sst_table = Arc::new(sst_builder.build(id, None, path)?);

        let state_lock = self.state_lock.lock();
        // check whether the memtable has been flushed by other threads by checking if the id is still in the imm_memtables
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard) // TODO: Understand this.
        };
        let id_vec: Vec<usize> = snapshot
            .imm_memtables
            .iter()
            .map(|table| table.id())
            .collect();
        if !id_vec.contains(&memtable_to_flush.id()) {
            return Ok(()); // the memtable has been flushed by other threads, return
        } else {
            let mut write_guard = self.state.write();
            let mut snapshot = write_guard.as_ref().clone();
            snapshot
                .imm_memtables
                .retain(|x| x.id() != memtable_to_flush.id());
            snapshot.l0_sstables.insert(0, id);
            snapshot.sstables.insert(id, new_sst_table);
            *write_guard = Arc::new(snapshot);
        }
        drop(state_lock);
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard) // TODO: Understand this.
        };

        // create the MemTableIterator part
        let mut memtable_iter_vec = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        memtable_iter_vec.push(Box::new(snapshot.memtable.scan(lower, Bound::Unbounded))); // LSM Iterator will handle the upper bound
        memtable_iter_vec.extend(
            snapshot
                .imm_memtables
                .iter()
                .map(|table| Box::new(table.scan(lower, Bound::Unbounded))), // LSM Iterator will handle the upper bound
        );
        let memtable_merge_iterator = MergeIterator::create(memtable_iter_vec);

        // create the SsTableIterator part
        // filter out the SSTs that are not in the range (optimization)
        let sst_id_within_range: Vec<usize> = snapshot
            .l0_sstables
            .iter()
            .filter(|x| {
                let sst = snapshot.sstables.get(x).unwrap();
                match (lower, upper) {
                    (Bound::Included(lower), Bound::Included(upper)) => {
                        sst.first_key().as_key_slice() <= KeySlice::from_slice(upper)
                            && sst.last_key().as_key_slice() >= KeySlice::from_slice(lower)
                    }
                    (Bound::Included(lower), Bound::Excluded(upper)) => {
                        sst.first_key().as_key_slice() < KeySlice::from_slice(upper)
                            && sst.last_key().as_key_slice() >= KeySlice::from_slice(lower)
                    }
                    (Bound::Excluded(lower), Bound::Included(upper)) => {
                        sst.first_key().as_key_slice() <= KeySlice::from_slice(upper)
                            && sst.last_key().as_key_slice() > KeySlice::from_slice(lower)
                    }
                    (Bound::Excluded(lower), Bound::Excluded(upper)) => {
                        sst.first_key().as_key_slice() < KeySlice::from_slice(upper)
                            && sst.last_key().as_key_slice() > KeySlice::from_slice(lower)
                    }
                    (Bound::Unbounded, Bound::Included(upper)) => {
                        sst.first_key().as_key_slice() <= KeySlice::from_slice(upper)
                    }
                    (Bound::Unbounded, Bound::Excluded(upper)) => {
                        sst.first_key().as_key_slice() < KeySlice::from_slice(upper)
                    }
                    (Bound::Included(lower), Bound::Unbounded) => {
                        sst.last_key().as_key_slice() >= KeySlice::from_slice(lower)
                    }
                    (Bound::Excluded(lower), Bound::Unbounded) => {
                        sst.last_key().as_key_slice() > KeySlice::from_slice(lower)
                    }
                    (Bound::Unbounded, Bound::Unbounded) => true,
                }
            })
            .map(|x| *x)
            .collect();
        let mut sst_iter_vec = Vec::with_capacity(sst_id_within_range.len());
        sst_iter_vec.extend(sst_id_within_range.iter().map(|sst_id| {
            let sst = snapshot.sstables.get(sst_id).unwrap();
            let mut inside_iter;
            match lower {
                Bound::Included(key) => {
                    inside_iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )
                    .unwrap();
                }
                Bound::Excluded(key) => {
                    inside_iter = SsTableIterator::create_and_seek_to_key(
                        Arc::clone(sst),
                        KeySlice::from_slice(key),
                    )
                    .unwrap();
                    inside_iter.next().unwrap();
                }
                Bound::Unbounded => {
                    inside_iter =
                        SsTableIterator::create_and_seek_to_first(Arc::clone(sst)).unwrap()
                }
            };
            Box::new(inside_iter)
        }));
        let sst_merge_iterator = MergeIterator::create(sst_iter_vec);

        // merge the two iterators into TwoMergeIterator
        let two_merge_iterator =
            TwoMergeIterator::create(memtable_merge_iterator, sst_merge_iterator).unwrap();
        Ok(FusedIterator::new(LsmIterator::new(
            two_merge_iterator,
            upper,
        )?))
    }
}
