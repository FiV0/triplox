# 25,000-node triangle query profiles after estimate caching

These profiles measure the standard and DBSP triangle queries after two
standard-engine changes in `src/query/patterns/triple.rs`:

- group rows with the same bound prefix before estimating proposal counts;
- cache prefix estimates for the lifetime of one `execute_query` call.

The server was launched through:

```bash
cargo run --release -- config/triplox-dev.toml
```

Samply wrapped that command and sampled at 999 Hz. The release build used
`CARGO_PROFILE_RELEASE_DEBUG=2` and
`RUSTFLAGS="-C force-frame-pointers=yes"` for better stack traces.

The benchmark used one long-lived dev-server connection. It ingested 25,000
vertices and 308,210 directed edges with probability
`9.859787813253787e-4`, then ran the standard query followed by the DBSP query
against the same graph. Both queries returned 2,510 rows.

| Query | Elapsed | CPU samples |
| --- | ---: | ---: |
| Standard `tc/q` | 6.243 s | 6,284 |
| DBSP `tc/dbsp-q` | 5.775 s | 10,091 |

The earlier profile at the same vertex count used a different random graph
with 307,791 edges and 2,488 result rows. Its standard query took 9.218 s and
its DBSP query took 5.715 s. The new standard timing is 1.48x faster despite
the slightly larger graph, while the DBSP timing is effectively unchanged.
These are single sampled runs rather than timing distributions.

In the corrected standard profile, `TriplePattern::count` fell from 33.0% of
samples to 1.7%, and `RangeStats::estimate_key_count` fell from 31.8% to 1.4%.
The dominant remaining work is candidate enumeration and storage iteration:
`candidate_extensions` accounts for 20.9%, `TemporalFilterIterator` for
23.4%, and Tokio `Handle::block_on` for 32.7%. Those inclusive shares overlap.

The DBSP profile remains split between `dbsp-worker-0` at 51.9% and `merger-0`
at 45.0%. `MergeWorkers` accounts for 44.7%, `ListMerger` for 29.8%, and file
writer code for 17.7%.

`gecko_profile_to_folded.py` maps every child-process sample against the root
profile time origin. This matters when Samply wraps `cargo run`: using each
child's startup time shifts late-created DBSP threads into the preceding
standard-query window.

Artifacts:

- `standard.svg` and `dbsp.svg`: Inferno flamegraphs;
- `standard.folded` and `dbsp.folded`: symbolized query-window stacks;
- `combined.profile.json.gz`: raw Samply profile containing both queries;
- `gecko_profile_to_folded.py`: wall-time window extraction and symbolization.
