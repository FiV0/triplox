import xyz.triplox.client.Delta;
import xyz.triplox.client.Row;
import xyz.triplox.client.Subscription;
import xyz.triplox.client.TriploxNode;
import xyz.triplox.client.TxOp;

import java.util.concurrent.TimeUnit;

import static xyz.triplox.client.Util.kw;
import static xyz.triplox.client.Util.list;
import static xyz.triplox.client.Util.map;

/**
 * Subscribe to an incremental query and print result deltas as transactions
 * arrive. Run against the default in-memory server (see examples/README.md).
 */
public final class StreamingExample {
    private StreamingExample() {
    }

    public static void main(String[] args) throws Exception {
        var host = "localhost";
        var port = 5490;
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (var node = TriploxNode.connect(host, port)) {
            System.out.println("Connected.");

            var schemaResult = node.executeTx(list(new TxOp.Put(map(
                    ":db/ident", kw(":name"),
                    ":db/valueType", kw(":db.type/string"),
                    ":db/cardinality", kw(":db.cardinality/one")))));
            if (!schemaResult.isCommitted()) {
                throw new IllegalStateException("Schema transaction aborted: " + schemaResult.errorMessage());
            }
            System.out.println("Schema defined.");

            // Subscribe at the latest indexed basis; closing the subscription unsubscribes.
            try (Subscription sub = node.subscribe("[:find ?name :where [?e :name ?name]]")) {
                System.out.println("Subscribed at tx_id=" + sub.txKey().txId() + ".");

                for (var name : new String[] {"alice", "bob", "carol"}) {
                    node.executeTx(list(new TxOp.Put(map(":name", name))));
                }

                System.out.println("Waiting for deltas...");
                for (int i = 0; i < 3; i++) {
                    Delta delta = sub.poll(5, TimeUnit.SECONDS);
                    if (delta == null) {
                        System.out.println("No more deltas.");
                        break;
                    }
                    for (Row row : delta.rows()) {
                        System.out.println("  " + row.values() + "  (weight " + row.weight() + ")");
                    }
                }
            }
        }

        System.out.println("Done.");
    }
}
