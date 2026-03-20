package io.triplox.client.integration;

import clojure.lang.Keyword;
import io.triplox.client.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

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
            node.executeTx(List.of(new TxOp.Put(Map.of(
                    "db/id", 200L,
                    "db/ident", Keyword.intern("name"),
                    "db/valueType", Keyword.intern("db.type", "string"),
                    "db/cardinality", 30L))));

            // Data
            var txResult = node.executeTx(List.of(new TxOp.Put(
                    Map.of("db/id", 1000L, "name", "alice"))));
            assertTrue(txResult.isCommitted());

            // Query
            try (var db = node.openDb()) {
                var rows = db.query("{:find [?name] :where [[?e :name ?name]]}");
                assertEquals(1, rows.size());
                assertEquals("alice", rows.get(0).get(0));
            }
        }
    }
}
