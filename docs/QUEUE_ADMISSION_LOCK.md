# Queue Admission Lock Design

## Goal

Provide a strict global hard cap for active simulations under `allowed_root`, even when queued runs and direct `run-inp` launches happen at nearly the same time.

Today, `QueueWorker._fill_slots()` counts active `run.lock` files and then dequeues the next pending job. That is good enough for the normal single-worker flow, but it is not atomic against concurrent direct starts.

## Current Gap

The current queue admission path is:

1. Count active `run.lock` files under `allowed_root`
2. Compare that count with `max_concurrent`
3. Dequeue one pending entry
4. Spawn `orca_auto run-inp --foreground`

If a direct `run-inp` starts between steps 1 and 4, the true active-run count can briefly exceed `max_concurrent`.

## Proposed Model

Add a separate admission subsystem under `allowed_root`:

- `admission.lock`
- `admission_slots.json`

The queue and per-reaction `run.lock` keep their current jobs:

- `queue.lock` still protects queue state transitions
- `run.lock` still prevents two runs inside the same reaction directory
- `admission.lock` becomes the single source of truth for the global slot count

## Slot Record Shape

Each record in `admission_slots.json` should contain:

- `token`: unique lease or reservation ID
- `state`: `reserved` or `active`
- `reaction_dir`
- `queue_id`
- `owner_pid`
- `process_start_ticks`
- `source`: `queue_worker` or `direct_run`
- `acquired_at`

`process_start_ticks` should be recorded anywhere PID liveness is used, just like the existing lock files, so stale slot cleanup is safe across PID reuse.

## State Machine

- `reserved`: a queue worker has claimed capacity but has not yet handed it off to the child process
- `active`: a running `run-inp` process owns the slot
- released: the record is removed from `admission_slots.json`

## Direct `run-inp` Flow

Future direct-start flow:

1. Validate the reaction directory
2. Recover crashed run state if needed
3. Acquire the per-directory `run.lock`
4. Acquire one global admission slot under `admission.lock`
5. Run ORCA
6. Release the admission slot in `finally`
7. Release the per-directory `run.lock`

This order avoids burning a scarce global slot while waiting on a conflicting `run.lock`.

If no slot is available, `run-inp` should fail fast with a clear message first. A later enhancement could add an explicit wait mode.

## Queue Worker Flow

Future queued-start flow:

1. Under `admission.lock`, reserve one slot if the total active or reserved count is below `max_concurrent`
2. Dequeue the next pending queue entry
3. If no queue entry exists, release the reservation immediately
4. Spawn `orca_auto run-inp --foreground` with the reservation token in an environment variable
5. In the child process, acquire `run.lock`
6. Promote the reservation from `reserved` to `active` and bind it to the child PID plus `process_start_ticks`
7. On normal or abnormal child exit, release the slot

This keeps the global cap atomic for the queue path while still letting `run-inp` own the final active lease.

## Why Reservation Instead Of Counting `run.lock`

Counting `run.lock` files is observational. A reservation is transactional.

With reservations:

- the worker claims capacity before it starts a child
- direct runs claim capacity before they begin execution
- every path uses the same shared limit
- stale reservations can be cleaned up by PID liveness checks

## Recovery Rules

Every admission operation should reconcile stale records first:

- if `owner_pid` is gone, remove the record
- if `process_start_ticks` does not match, remove the record
- if a reservation belongs to a dead worker process, remove the record

Queue worker startup should also clean dead `reserved` entries before its first scheduling pass.

## Lock Ordering

To avoid deadlocks:

- direct `run-inp`: `run.lock` then `admission.lock`
- queue worker: short `admission.lock` reservation step, then `queue.lock`
- avoid holding `queue.lock` and `admission.lock` at the same time unless an implementation step truly requires it

The current single-worker design means queue contention is already low, so keeping the admission critical section short is more important than combining everything into one large lock.

## Suggested Module Boundary

Add a future `core/admission_store.py` with a small API:

- `reserve_slot(allowed_root, *, max_concurrent, owner_pid, queue_id, reaction_dir) -> str | None`
- `activate_slot(allowed_root, token, *, owner_pid, process_start_ticks, reaction_dir) -> bool`
- `release_slot(allowed_root, token) -> bool`
- `acquire_direct_slot(allowed_root, *, max_concurrent, reaction_dir) -> context manager`
- `reconcile_stale_slots(allowed_root) -> int`

This mirrors the existing `queue_store` and `state_store` split and fits the current codebase style well.

## Integration Notes

- `max_concurrent` is shared through `runtime.max_concurrent` in config for both queue and direct runs
- future CPU or RAM-based scheduling can sit on top of admission slots; the slot system should answer only "is there capacity to admit another run?"
- queue ordering should remain in `queue_store`; admission slots should not decide which job runs next
