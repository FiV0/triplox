package xyz.triplox.client;

import okhttp3.Protocol;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("integration")
class IntegrationHttpProtocolTest {

    private static String host() {
        return System.getProperty("triplox.host", "localhost");
    }

    private static int port() {
        return Integer.parseInt(System.getProperty("triplox.port", "5490"));
    }

    @Test
    void usesHttp2PriorKnowledge() throws Exception {
        try (var node = TriploxNode.connect(host(), port())) {
            node.openDb();
            assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, node.lastProtocol());
        }
    }
}
