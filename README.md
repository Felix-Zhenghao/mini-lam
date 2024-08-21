# Notes
> In this project, all keys are sorted from small ones to big ones. (asc)

## W1 D1

Why do we need a combination of `state` and `state_lock`? Can we only use `state.read()` and `state.write()`?
- First reason. We get the state and check if update is needed. However, during we go from get the `Update_needed` state to check, other threads may get the `Update_needed` state as well. So all these threads will finally update the state after check. In this case, we might create one empty memtable which is then immediately frozen. This can be solved by `mutex`. (see the code)
- Second reason. To separate the expensive write lock and the lock needed for checking. Without `mutex`, we have to do check in the write lock. With `mutex`, we can first `mutex.lock()` the thread that want to do further check, where the write_lock has not been validated. After the check, the write_lock is validated.

## W1 D2
The LSM now has one mutable memtable and many archived memtables. So, each memtable has one iterator and LSM has an iterator by merging all memtable iterators.

The skipmap is sorted, so the memtable iterators are originally sorted. To iterate over all k-v pairs in all memtables, order is defined as 'iterate from bigger key to smaller key, and if a key exists multiple times, get the latest value'. According to this order, memtable iterators are wrapped by a binary heap, and together sent and managed by a merged iterator.

Memtable iter is in `mem_table.rs` called `MemTableIterator`. Merged iter is in `iterators/merge_iterator.rs` called `MergeIterator`. The joint usage of data structure like max-heap and skip-map grant the local order and global order, which is beautiful. You should review it.

Also, every iterator is inherited from an ABC:
```rust
pub trait StorageIterator {
    type KeyType<'a>: PartialEq + Eq + PartialOrd + Ord

    /// Get the current value.
    fn value(&self) -> &[u8];

    /// Get the current key.
    fn key(&self) -> Self::KeyType<'_>;

    /// Check if the current iterator is valid.
    fn is_valid(&self) -> bool;

    /// Move to the next position.
    fn next(&mut self) -> anyhow::Result<()>;

    /// Number of underlying active iterators for this iterator.
    fn num_active_iterators(&self) -> usize {
        1
    }
}
```

## W1 D3
In this sector, the data stored in disk is very **compact**. Once you fix the byte size of offset and k_len and v_len, you only need `num_of_elements` as the extra information to get all data. See the implementation in `src/block.rs`. And this allows us to get key/value through slice rather than cloning everything.
```
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

AND each entry is a k-v pair:

-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------
```


## W1 D4
Now the memory structure of this project become: when data coming in, they'll be written to memory table. For some time, memory table will be somehow be transferred to the disk. On the disk, data is organized as data blocks (permanent data stored outside), and several data blocks become an SST.

**You also need to undersand how to implement cache using `moka` crate.**


## W1 D5
The reaction when call next on invalid iterator, including `MemTableIterator`, `MergeIterator`, `BlockIterator`, `SsTableIterator`, `TwoMergeIterator`, is `self.key()` and `self.value()` will both be `&[]` and won't return `Err`.

However, in `lsm_iterator.rs`, if you wrap any iterator using `FusedIterator`, the underlying iterator will do nothing when called `next()` when it is invalid. This makes the code more efficient.