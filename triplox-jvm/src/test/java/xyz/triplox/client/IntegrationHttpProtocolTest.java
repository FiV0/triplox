package xyz.triplox.client;

import okhttp3.Call;
import okhttp3.EventListener;
import okhttp3.Protocol;
import okhttp3.Response;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

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
        var observedProtocol = new AtomicReference<Protocol>();
        var client = TriploxNode.httpClientBuilder()
                .eventListener(new EventListener() {
                    @Override
                    public void responseHeadersEnd(Call call, Response response) {
                        observedProtocol.set(response.protocol());
                    }
                })
                .build();

        try (var node = TriploxNode.connect(host(), port(), client)) {
            node.openDb();
            assertEquals(Protocol.H2_PRIOR_KNOWLEDGE, observedProtocol.get());
        }
    }
}
