# Source attribution

The source loader, CSV reader, query definitions, and expected answers are
adapted from Datalevin commit `fcab56d2fb7d7137011b125a78c0383b9dbb3816`:

- `benchmarks/JOB-bench/src/datalevin_bench/core.clj`
- `benchmarks/JOB-bench/test/datalevin_bench/core_test.clj`
- `src/java/datalevin/utl/CSVReader.java`

These files are covered by the included Eclipse Public License 2.0. The loader
emits plain triples instead of Datalevin datoms; the CSV reader's package is
changed. The upstream query resource preserves the original expressions.

JOB originates from Viktor Leis et al., *How Good Are Query Optimizers, Really?*,
PVLDB 9(3), 2015. Data source: https://event.cwi.nl/da/job/imdb.tgz.

The source snapshot above and the pinned Datalevin runtime version are recorded
separately: this benchmark runs released Datalevin 1.1.0.
