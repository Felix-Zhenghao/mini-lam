![banner](./mini-lsm-book/src/mini-lsm-logo.png)

# Notes

## W1 D1

Why do we need a combination of `state` and `state_lock`? Can we only use `state.read()` and `state.write()`?
- First reason. We get the state and check if update is needed. However, during we go from get the `Update_needed` state to check, other threads may get the `Update_needed` state as well. So all these threads will finally update the state after check. In this case, we might create one empty memtable which is then immediately frozen. This can be solved by `mutex`. (see the code)
- Second reason. To separate the expensive write lock and the lock needed for checking. Without `mutex`, we have to do check in the write lock. With `mutex`, we can first `mutex.lock()` the thread that want to do further check, where the write_lock has not been validated. After the check, the write_lock is validated.

