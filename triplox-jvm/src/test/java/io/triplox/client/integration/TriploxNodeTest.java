package io.triplox.client.integration;

import clojure.lang.Keyword;
import io.triplox.client.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
            var schema = new TreeMap<Keyword, Object>();
            schema.put(Keyword.intern("db", "id"), 200L);
            schema.put(Keyword.intern("db", "ident"), Keyword.intern("name"));
            schema.put(Keyword.intern("db", "valueType"), Keyword.intern("db.type", "string"));
            schema.put(Keyword.intern("db", "cardinality"), Keyword.intern("db.cardinality", "one"));
            node.executeTx(List.of(new TxOp.Put(schema)));

            // Data
            var data = new TreeMap<Keyword, Object>();
            data.put(Keyword.intern("db", "id"), 1000L);
            data.put(Keyword.intern("name"), "alice");
            var txResult = node.executeTx(List.of(new TxOp.Put(data)));
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
