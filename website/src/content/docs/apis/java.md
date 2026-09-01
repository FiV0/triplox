---
title: Java API
description: The Triplox Java client.
---

The Triplox Java client is published on [Maven Central](https://central.sonatype.com/artifact/xyz.triplox/triplox) under `xyz.triplox:triplox`.
The full API reference is available at [javadoc.io](https://javadoc.io/doc/xyz.triplox/triplox/latest/xyz/triplox/client/package-summary.html).

## Installation

With Gradle (`build.gradle.kts`):

```kotlin
dependencies {
    implementation("xyz.triplox:triplox:0.1.0-alpha.4")
}
```

With Maven (`pom.xml`):

```xml
<dependency>
    <groupId>xyz.triplox</groupId>
    <artifactId>triplox</artifactId>
    <version>0.1.0-alpha.4</version>
</dependency>
```

The client requires Java 21 or later.

## Example

The following program defines a small schema, inserts two entities, and runs a Datalog query against a db value.
You will need a running Triplox server. See the [quick start](/getting-started/quick-start/) for how to launch one.


```java
import xyz.triplox.client.TriploxNode;
import xyz.triplox.client.TxOp;

import static xyz.triplox.client.Util.kw;
import static xyz.triplox.client.Util.list;
import static xyz.triplox.client.Util.map;

public final class SimpleExample {
    private SimpleExample() {
    }

    public static void main(String[] args) throws Exception {
        var host = "localhost";
        var port = 5490;
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (var node = TriploxNode.connect(host, port)) {
            System.out.println("Connected.");

            var schemaResult = node.executeTx(list(
                    new TxOp.Put(map(
                            ":db/ident", kw(":name"),
                            ":db/valueType", kw(":db.type/string"),
                            ":db/cardinality", kw(":db.cardinality/one"))),
                    new TxOp.Put(map(
                            ":db/ident", kw(":age"),
                            ":db/valueType", kw(":db.type/long"),
                            ":db/cardinality", kw(":db.cardinality/one")))));
            if (!schemaResult.isCommitted()) {
                throw new IllegalStateException("Schema transaction aborted: " + schemaResult.errorMessage());
            }
            System.out.println("Schema defined (tx_id=" + schemaResult.txId() + ").");

            var dataResult = node.executeTx(list(
                    new TxOp.Put(map(":name", "alice", ":age", 30L)),
                    new TxOp.Put(map(":name", "bob", ":age", 25L))));
            if (!dataResult.isCommitted()) {
                throw new IllegalStateException("Data transaction aborted: " + dataResult.errorMessage());
            }
            System.out.println("Data inserted (tx_id=" + dataResult.txId() + ").");

            var db = node.openDb();
            System.out.println("Opened DB value (tx_eid=" + db.txEid() + ").");
            var rows = db.query("""
                    {:find [?e ?name ?age]
                     :where [[?e :name ?name]
                             [?e :age ?age]]}
                    """);

            System.out.println("Query returned " + rows.size() + " row(s):");
            for (var row : rows) {
                System.out.println("  " + row);
            }
        }

        System.out.println("Done.");
    }
}
```
