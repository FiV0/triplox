package xyz.triplox.client.integration;

import xyz.triplox.client.*;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static xyz.triplox.client.Util.kw;
import static xyz.triplox.client.Util.map;
import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTest {

    private static String host() {
        return System.getProperty("triplox.host", "localhost");
    }

    private static int port() {
        return Integer.parseInt(System.getProperty("triplox.port", "5490"));
    }

    private static final String NAMES_QUERY = "[:find ?name :where [?e :name ?name]]";

    private static void defineNameSchema(TriploxNode node) throws Exception {
        node.executeTx(List.of(new TxOp.Put(map(
                ":db/ident", kw(":name"),
                ":db/valueType", kw(":db.type/string"),
                ":db/cardinality", kw(":db.cardinality/one")))));
    }

    @Test
    void testSubscribeReceivesDelta() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            defineNameSchema(node);
            try (Subscription sub = node.subscribe(NAMES_QUERY)) {
                assertNotNull(sub.txKey());

                node.executeTx(List.of(new TxOp.Put(map(":name", "Ivan"))));

                Delta delta = sub.poll(10, TimeUnit.SECONDS);
                assertNotNull(delta, "expected a delta within 10s");
                assertEquals(1, delta.rows().size());
                assertEquals(List.of("Ivan"), delta.rows().get(0).values());
                assertEquals(1L, delta.rows().get(0).weight());
            }
        }
    }

    @Test
    void testSubscribeReturnsExistingRowsAsPrimingDelta() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            defineNameSchema(node);
            node.executeTx(List.of(new TxOp.Put(map(":name", "Alice"))));

            try (Subscription sub = node.subscribe(NAMES_QUERY)) {
                Delta delta = sub.poll(10, TimeUnit.SECONDS);
                assertNotNull(delta, "expected a priming delta within 10s");
                assertEquals(sub.txKey(), delta.txKey());
                assertEquals(1, delta.rows().size());
                assertEquals(List.of("Alice"), delta.rows().get(0).values());
                assertEquals(1L, delta.rows().get(0).weight());
            }
        }
    }

    @Test
    void testPollTimesOutWithoutChange() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            defineNameSchema(node);
            try (Subscription sub = node.subscribe(NAMES_QUERY)) {
                // No transaction after the subscription -> poll returns null on timeout.
                assertNull(sub.poll(300, TimeUnit.MILLISECONDS));
            }
        }
    }

    @Test
    void testUnsupportedQueryThrows() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            defineNameSchema(node);
            // Registration errors mirror standard query errors for now -> pre-stream 2001.
            var ex = assertThrows(TriploxException.class,
                    () -> node.subscribe("[:find ?n :in ?x :where [?e :name ?n]]"));
            assertEquals((short) 2001, ex.code());
        }
    }
}
