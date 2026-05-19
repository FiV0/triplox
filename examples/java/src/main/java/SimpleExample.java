import clojure.lang.Keyword;
import io.triplox.client.Db;
import io.triplox.client.TriploxNode;
import io.triplox.client.TxOp;
import io.triplox.client.TxResult;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public final class SimpleExample {
    private SimpleExample() {
    }

    private static Map<Keyword, Object> schemaAttribute(String name, String valueType) {
        var doc = new TreeMap<Keyword, Object>();
        doc.put(Keyword.intern("db", "ident"), Keyword.intern(name));
        doc.put(Keyword.intern("db", "valueType"), Keyword.intern("db.type", valueType));
        doc.put(Keyword.intern("db", "cardinality"), Keyword.intern("db.cardinality", "one"));
        return doc;
    }

    private static Map<Keyword, Object> person(String name, long age) {
        var doc = new TreeMap<Keyword, Object>();
        doc.put(Keyword.intern("name"), name);
        doc.put(Keyword.intern("age"), age);
        return doc;
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
                    new TxOp.Put(person("alice", 30)),
                    new TxOp.Put(person("bob", 25)))));
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
