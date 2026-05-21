package xyz.triplox.client.integration;

import xyz.triplox.client.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static xyz.triplox.client.Util.kw;
import static xyz.triplox.client.Util.map;
import static org.junit.jupiter.api.Assertions.*;

class TriploxNodeTest {

    private static String host() {
        return System.getProperty("triplox.host", "localhost");
    }

    private static int port() {
        return Integer.parseInt(System.getProperty("triplox.port", "5490"));
    }

    @Test
    void testConnectTransactQueryClose() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            // Schema: name attribute
            node.executeTx(List.of(new TxOp.Put(map(
                    ":db/ident", kw(":name"),
                    ":db/valueType", kw(":db.type/string"),
                    ":db/cardinality", kw(":db.cardinality/one")))));

            // Data
            var txResult = node.executeTx(List.of(new TxOp.Put(map(":name", "alice"))));
            assertTrue(txResult.isCommitted());

            // Query
            try (var db = node.openDb()) {
                var rows = db.query("{:find [?name] :where [[?e :name ?name]]}");
                assertEquals(1, rows.size());
                assertEquals("alice", rows.get(0).get(0));
            }
        }
    }

    @Test
    void testOpenDbAtHistoricalTx() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            node.executeTx(List.of(new TxOp.Put(map(
                    ":db/ident", kw(":historical-name"),
                    ":db/valueType", kw(":db.type/string"),
                    ":db/cardinality", kw(":db.cardinality/one")))));

            var tx1 = node.executeTx(List.of(new TxOp.Put(map(":historical-name", "alice"))));
            assertTrue(tx1.isCommitted());

            var tx2 = node.executeTx(List.of(new TxOp.Put(map(":historical-name", "bob"))));
            assertTrue(tx2.isCommitted());

            try (var db = node.openDbAsOf(tx1.basis())) {
                var rows = db.query("{:find [?name] :where [[?e :historical-name ?name]]}");
                assertEquals(List.of(List.of("alice")), rows);
            }
        }
    }
}
