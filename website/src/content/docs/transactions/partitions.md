---
title: Partitions
description: Triplox partitions.
---

Entities are assigned to a particular partition. Partitions allow the grouping of entities
that are loosly related. Tenants are a good example where different partitions make sense.
The highest 20 bits of entity ids are reserved for partitions. The main point of partitions
is data locality when joining. Related entities are grouped together and related data is
fetched together from object storage. Simplifying a lot things, you are likely needing
less file fetches from object storage when doing an EAV scan for a particular tenant.


Triplox currently supports 3 partitions:
- A db partition that holds schema entities and bootstrap entities
that define the schema and describe Triplox internal entities.
- A transaction partition. Every transaction gets an auto-assigned transaction entity sitting in the transaction partition.
See [transaction model](/transactions/transaction-model/) for what default attributes
a transaction entity holds.
- A user partition that holds all user defined entities (apart from schema entities). This is the default partition.

We plan to add a mechanism for assigning partitions via user transactions. See the [Roadmap](/roadmap/roadmap).

:::note
This partition assignment schema is very much inspired by Datomic.
:::
