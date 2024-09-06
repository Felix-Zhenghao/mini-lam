#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    /* REMINDER OF LsmStorageState
    pub struct LsmStorageState {
        pub memtable: Arc<MemTable>,
        pub imm_memtables: Vec<Arc<MemTable>>,
        pub l0_sstables: Vec<usize>,
        pub levels: Vec<(usize, Vec<usize>)>,
        pub sstables: HashMap<usize, Arc<SsTable>>,
    }
    */
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        // get the merge iterator of all blocks in current sstables
        let mut iter = match _task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sst_table_vec: Vec<Box<SsTableIterator>> =
                    Vec::with_capacity(l0_sstables.len() + l1_sstables.len());
                l0_sstables.iter().for_each(|&sst_id| {
                    sst_table_vec.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(&sst_id).unwrap().clone(),
                        )
                        .unwrap(),
                    ));
                });
                l1_sstables.iter().for_each(|&sst_id| {
                    sst_table_vec.push(Box::new(
                        SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(&sst_id).unwrap().clone(),
                        )
                        .unwrap(),
                    ));
                });
                MergeIterator::create(sst_table_vec)
            }
            _ => unimplemented!(),
        };

        // use the merge iterator created above to build a set of new sorted sstables
        let mut new_sst_builder = SsTableBuilder::new(self.options.block_size);
        let mut output: Vec<Arc<SsTable>> = Vec::new();
        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            if new_sst_builder.estimated_size() >= self.options.target_sst_size {
                let new_builder = SsTableBuilder::new(self.options.block_size);
                let old_builder = std::mem::replace(&mut new_sst_builder, new_builder);
                let index = self.next_sst_id();
                let new_sst = old_builder.build(index, None, self.path_of_sst(index))?;
                output.push(Arc::new(new_sst));
            }
            if !value.is_empty() {
                new_sst_builder.add(key, value);
            }
            iter.next()?;
        }

        let new_builder = SsTableBuilder::new(self.options.block_size);
        let old_builder = std::mem::replace(&mut new_sst_builder, new_builder);
        let index = self.next_sst_id();
        let new_sst = old_builder.build(index, None, self.path_of_sst(index))?;
        output.push(Arc::new(new_sst));

        Ok(output)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let sstables = self.compact(&compaction_task)?;

        {
            // since the last read lock, the state might have changed, so we need to re-read the state and get up-to-date snapshot
            let _state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();

            // remove old sstabels from state.sstabels
            for sst_id in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = snapshot.sstables.remove(sst_id);
                assert!(result.is_some());
            }

            // insert the sstables after merge sort to the hashmap
            let mut ids = Vec::with_capacity(sstables.len());
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = snapshot.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }

            // update the L1 ids - move all sorted sstables to L1
            snapshot.levels[0].1 = ids;

            // update the l0_sstabels, may leave some newly come-in sstables in l0 or cleared
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            snapshot.l0_sstables = snapshot
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());

            *self.state.write() = Arc::new(snapshot);
        }

        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        if self.state.read().imm_memtables.len() >= self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
