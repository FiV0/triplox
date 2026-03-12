package io.triplox.client.integration;

import clojure.lang.Keyword;
import io.triplox.client.*;
import org.junit.jupiter.api.Test;

import java.util.List;
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
            var schema = new TreeMap<String, Object>();
            schema.put("db/id", 50L);
            schema.put("db/ident", Keyword.intern("name"));
            schema.put("db/valueType", Keyword.intern("db.type", "string"));
            node.executeTx(List.of(new TxOp.Put(schema)));

            // Data
            var doc = new TreeMap<String, Object>();
            doc.put("db/id", 100L);
            doc.put("name", "alice");
            var txResult = node.executeTx(List.of(new TxOp.Put(doc)));
            assertTrue(txResult.isCommitted());

            // Query
            try (var db = node.openDb()) {
                var result = db.query("{:find [?name] :where [[?e :name ?name]]}");
                assertEquals(1, result.rows().size());
                assertEquals("alice", result.rows().get(0).get(0));
            }
        }
    }
}
