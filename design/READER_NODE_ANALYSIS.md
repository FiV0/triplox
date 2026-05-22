# Reader Node Analysis: SlateDB Change Detection

## Context

ReaderNode should be driven by what a `slatedb::DbReader` can actually observe. The in-memory data structures should follow from SlateDB's change notification model rather than assuming a Triplox-specific event stream.

This analysis is based on the current Triplox dependency on SlateDB plus the proposed SlateDB feature in https://github.com/FiV0/slatedb/pull/2.

## Current SlateDB Status Model

`DbStatus` is exposed through a Tokio `watch::Receiver<DbStatus>`, not through an append-only event log. A watch channel is level-triggered: receivers observe the latest value, and intermediate changes can be coalesced.

Current `DbStatus` contains:

- `durable_seq`: the durable sequence number visible through the handle.
- `current_manifest`: the manifest snapshot observed by this handle.
- `close_reason`: set when the handle closes.

`DbStatusManager` publishes updates with `send_if_modified`. A status notification is emitted only when a status field actually changes. In the current dependency that means durable sequence advances, manifest changes, or close reason is set.

`DbReader::subscribe()` returns this watch receiver. `DbReader::status()` returns a clone of the current status. The `DbMetadataOps` docs warn not to hold a borrowed watch value across `.await`; clone or copy what is needed immediately.

## How DbReader Detects Changes

A `DbReader` without an explicit checkpoint spawns a manifest poller. The poll interval comes from `DbReaderOptions.manifest_poll_interval`, which defaults to 10 seconds.

On each poll, the reader loads the latest manifest and then does one of two things:

1. Reestablishes the checkpoint if manifest state requires it.
2. Otherwise replays new WAL files into the reader state.

Reestablishing the checkpoint can update both the reader's durable sequence and manifest status. Replaying WAL files can update readable data even when the manifest does not change.

If a reader is opened with an explicit checkpoint, the SlateDB docs indicate that the manifest and WAL are not polled. That mode is not suitable for a live ReaderNode that should follow the writer.

## Effect of `last_replayed_wal_id`

PR https://github.com/FiV0/slatedb/pull/2 adds `DbStatus.last_replayed_wal_id`. The PR describes it as the largest WAL ID whose contents are readable through the handle.

With that feature, `DbStatus` becomes a more precise invalidation signal:

- `last_replayed_wal_id` advances after a writer WAL flush or after `DbReader` WAL replay.
- It can advance independently of `durable_seq`, `current_manifest`, and `close_reason`.
- For ReaderNode, it is the clearest signal that reader-visible WAL-backed Triplox index data may have changed.

This does not remove the need to derive Triplox state from indexes. `DbStatus` still does not contain a Triplox `TxBasis`, schema, or partition metadata.

## Implications for ReaderNode

ReaderNode should treat `DbStatus` as an invalidation signal and visibility watermark, not as the ReaderNode state itself.

The status loop should:

1. Subscribe to `DbReader` status.
2. Clone the current status immediately.
3. Refresh derived Triplox state from reader-visible indexes.
4. Wait for `changed()`.
5. Clone the new status immediately.
6. Decide whether the status is relevant.
7. Refresh derived Triplox state if needed.

Because `watch` coalesces updates, the refresh logic must be idempotent. It should not assume it saw every WAL or manifest transition. The correct model is: "something relevant may have changed, recompute the latest reader-visible Triplox state."

With `last_replayed_wal_id`, ReaderNode can remember the last observed SlateDB visibility fields to avoid unnecessary scans:

- `last_replayed_wal_id`
- `current_manifest.id`
- `durable_seq`
- `close_reason`

That observed-status record is different from the derived Triplox state. It is a cache/invalidation cursor for deciding whether to rescan.

## Suggested Refresh Shape

Illustrative structure only:

```rust
struct ObservedDbStatus {
    durable_seq: u64,
    manifest_id: u64,
    last_replayed_wal_id: u64,
    closed: bool,
}
```

The subscription loop should be shaped like this:

```rust
let mut status_rx = reader.subscribe();

let status = status_rx.borrow_and_update().clone();
refresh_from_reader_visible_indexes(status).await?;

tokio::spawn(async move {
    while status_rx.changed().await.is_ok() {
        let status = status_rx.borrow_and_update().clone();

        if status.close_reason.is_some() {
            break;
        }

        if let Err(err) = refresh_from_reader_visible_indexes(status).await {
            // Keep serving the last known good derived state and record the error.
        }
    }
});
```

The key rule is that `borrow()` or `borrow_and_update()` must not be held across an `.await`.

## What ReaderNode Should Derive

After a relevant status change, ReaderNode should derive Triplox state by reading through the same `DbReader` view that query execution will use.

Likely derived state includes:

- Latest reader-visible `TxBasis`, from Triplox transaction indexes.
- Query schema / ident map, from reader-visible schema indexes.
- Possibly partition metadata if ReaderNode later needs it for read-side features.

The exact in-memory structure should be chosen after implementing the refresh path. It may be a single atomically swapped snapshot, separate locks for independently used data, or an internal refresh worker with a watch channel of derived Triplox state. The SlateDB side suggests an atomic derived snapshot is attractive, but the design should be validated against actual query and server call sites.

## Open Design Questions

- Should ReaderNode rescan schema on every relevant status change, or first detect whether the latest visible transaction could contain schema changes?
- Should ReaderNode publish derived state through its own `watch` channel so DB open/query handlers can wait for refresh completion?
- If refresh fails after a SlateDB status change, should ReaderNode continue serving the last known good basis, reject new DB opens, or mark itself unhealthy?
- Should `DbReaderOptions.manifest_poll_interval` be configurable from Triplox reader-node config?
- Should ReaderNode require `skip_wal_replay = false` for live mode?

## Current Working Assumption

ReaderNode should open a live, polling `DbReader` without an explicit checkpoint and with WAL replay enabled. It should subscribe to `DbStatus`, use status changes as invalidation, and maintain Triplox-derived read state computed from reader-visible indexes.
