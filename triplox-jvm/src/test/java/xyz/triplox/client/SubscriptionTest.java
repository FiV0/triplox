package xyz.triplox.client;

import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.Interceptor;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTest {

    @Test
    void subscriptionInterceptorDisablesReadTimeoutOnlyForSubscribe() throws IOException {
        var subscribeChain = new RecordingChain(request("/db/subscribe"));
        TriploxNode.disableReadTimeoutForSubscriptions(subscribeChain);
        assertEquals(0, subscribeChain.readTimeoutMillis());
        assertTrue(subscribeChain.proceeded);

        var queryChain = new RecordingChain(request("/db/query"));
        TriploxNode.disableReadTimeoutForSubscriptions(queryChain);
        assertEquals(10_000, queryChain.readTimeoutMillis());
        assertTrue(queryChain.proceeded);
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

    private static Request request(String path) {
        return new Request.Builder()
                .url("http://localhost:5490" + path)
                .build();
    }

    private static final class RecordingChain implements Interceptor.Chain {
        private final Request request;
        private int readTimeoutMillis = 10_000;
        private boolean proceeded;

        private RecordingChain(Request request) {
            this.request = request;
        }

        @Override
        public Request request() {
            return request;
        }

        @Override
        public Response proceed(Request request) {
            proceeded = true;
            return new Response.Builder()
                    .request(request)
                    .protocol(Protocol.H2_PRIOR_KNOWLEDGE)
                    .code(200)
                    .message("OK")
                    .build();
        }

        @Override
        public Connection connection() {
            return null;
        }

        @Override
        public Call call() {
            return null;
        }

        @Override
        public int connectTimeoutMillis() {
            return 10_000;
        }

        @Override
        public Interceptor.Chain withConnectTimeout(int timeout, TimeUnit unit) {
            return this;
        }

        @Override
        public int readTimeoutMillis() {
            return readTimeoutMillis;
        }

        @Override
        public Interceptor.Chain withReadTimeout(int timeout, TimeUnit unit) {
            readTimeoutMillis = (int) unit.toMillis(timeout);
            return this;
        }

        @Override
        public int writeTimeoutMillis() {
            return 10_000;
        }

        @Override
        public Interceptor.Chain withWriteTimeout(int timeout, TimeUnit unit) {
            return this;
        }
    }
}
