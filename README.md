<p align="center">
  <img src="img/logo_thin.png" alt="Triplox logo" width="600">
</p>

# Triplox


### Architecture
```

                   ┌─────────────────────────────────────────────────────────────┐
                   │                   Object Storage (S3)                       │
                   │                                                             │
                   │  ┌─────────────┐      ┌─────────────┐      ┌─────────────┐  │
                   │  │   SlateDB   │      │   SlateDB   │      │   SlateDB   │  │
                   │  │  (Writer)   │      │  (Reader 1) │      │  (Reader 2) │  │
                   │  └──────┬──────┘      └──────┬──────┘      └──────┬──────┘  │
                   │         │                    │                    │         │
                   └─────────┼────────────────────┼────────────────────┼─────────┘
                             │                    │                    │
     Queries/Indices         ▲ read/write         ▼ read               ▼ read
                             │                    │                    │
        ┌────────────────────┴────────┐  ┌────────┴────────┐  ┌────────┴───────┐
        │         Writer Node         │  │  Reader Node 1  │  │  Reader Node 2 │
        │                             │  │                 │  │                │
        │      ┌──────────────┐       │  │                 │  │                │
  ┌─────┼────▶│   Indexer    │       │  │                 │  │                │
  │     │      └──────────────┘       │  │                 │  │                │
  │     │                             │  │                 │  │                │
  │     └─────────────┬───────────────┘  └─────────────────┘  └────────────────┘
  │                   │
  │  Transactions     │ write
  │                   ▼
  │     ┌──────────────────────────────────────────────────────────────────────┐
  │     │                                                                      │
  │     │                 Log (Kafka, S2, WAL3, etc.)                          │
  │     │                                                                      │
  │     │    ┌─────┬─────┬─────┬─────┬─────┬─────┬─────┬─────┐                 │
  └─────┼────┤ tx0 │ tx1 │ tx2 │ tx3 │ tx4 │ tx5 │ tx6 │ ... │                 │
   read │    └─────┴─────┴─────┴─────┴─────┴─────┴─────┴─────┘                 │
        │                                                                      │
        └──────────────────────────────────────────────────────────────────────┘
```

### Licence

Triplox is licensed under the Apache License, Version 2.0.

The [edn](edn/) crate was orginally copied from [mentat](https://github.com/mozilla/mentat) and is also licenced under Apache Licence, Version 2.0.
