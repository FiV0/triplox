package xyz.triplox.client;

import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTest {

    @Test
    void subscriptionClientDisablesReadTimeoutAndSharesConnectionPool() {
        var baseClient = new OkHttpClient.Builder()
                .protocols(List.of(Protocol.H2_PRIOR_KNOWLEDGE))
                .readTimeout(10, TimeUnit.SECONDS)
                .build();

        var subscriptionClient = TriploxNode.subscriptionClientFor(baseClient);

        assertEquals(0, subscriptionClient.readTimeoutMillis());
        assertSame(baseClient.connectionPool(), subscriptionClient.connectionPool());
    }

    @Test
    void unexpectedStreamIOExceptionSurfacesAsTerminalError() throws Exception {
        var unpacker = MessagePack.newDefaultUnpacker(new ThrowingInputStream());
        try (Subscription sub = new Subscription(null, () -> {}, unpacker)) {
            var ex = assertThrows(TriploxException.class, () -> sub.poll(5, TimeUnit.SECONDS));
            assertEquals(4000, Short.toUnsignedInt(ex.code()));
            assertTrue(ex.getMessage().contains("subscription stream failed"));
            assertNotNull(ex.getCause());
        }
    }

    private static final class ThrowingInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            throw new IOException("read failed");
        }
    }
}
