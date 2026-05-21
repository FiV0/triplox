import io.triplox.client.Db;
import io.triplox.client.TriploxNode;
import io.triplox.client.TxOp;
import io.triplox.client.TxResult;

import java.util.List;
import java.util.Map;

import static io.triplox.client.Util.kw;
import static io.triplox.client.Util.map;

public final class SimpleExample {
    private SimpleExample() {
    }

    private static Map<String, Object> schemaAttribute(String name, String valueType) {
        return map(
                ":db/ident", kw(":" + name),
                ":db/valueType", kw(":db.type/" + valueType),
                ":db/cardinality", kw(":db.cardinality/one"));
    }

    private static TxResult requireCommitted(String label, TxResult result) {
        if (!result.isCommitted()) {
            throw new IllegalStateException(label + " transaction aborted: " + result.errorMessage());
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        var host = "localhost";
        var port = 5490;
        System.out.println("Connecting to " + host + ":" + port + "...");

        try (var node = TriploxNode.connect(host, port)) {
            System.out.println("Connected.");

            var schemaResult = requireCommitted("Schema", node.executeTx(List.of(
                    new TxOp.Put(schemaAttribute("name", "string")),
                    new TxOp.Put(schemaAttribute("age", "long")))));
            System.out.println("Schema defined (tx_id=" + schemaResult.txId() + ").");

            var dataResult = requireCommitted("Data", node.executeTx(List.of(
                    new TxOp.Put(map(":name", "alice", ":age", 30L)),
                    new TxOp.Put(map(":name", "bob", ":age", 25L)))));
            System.out.println("Data inserted (tx_id=" + dataResult.txId() + ").");

            try (Db db = node.openDb()) {
                System.out.println("Opened DB snapshot (tx_eid=" + db.txEid() + ").");
                var rows = db.query("{:find [?e ?name ?age] :where [[?e :name ?name] [?e :age ?age]]}");

                System.out.println("Query returned " + rows.size() + " row(s):");
                for (var row : rows) {
                    System.out.println("  " + row);
                }
            }
        }

        System.out.println("Done.");
    }
}
