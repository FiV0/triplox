package xyz.triplox.client;

import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.Interceptor;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
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

    @Test
    void openFrameMidStreamSurfacesAsInvalidState() throws Exception {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packOpenFrame(packer);
            packOpenFrame(packer);
            body = packer.toByteArray();
        }

        var unpacker = MessagePack.newDefaultUnpacker(new ByteArrayInputStream(body));
        var first = assertInstanceOf(SubscriptionFrame.Open.class, WireCodec.decodeSubscriptionFrame(unpacker));
        try (Subscription sub = new Subscription(first.basis(), () -> {}, unpacker)) {
            var ex = assertThrows(IllegalStateException.class, () -> sub.poll(5, TimeUnit.SECONDS));
            assertTrue(ex.getMessage().contains("unexpected open frame mid-stream"));
        }
    }

    @Test
    void closeDiscardsQueuedDeltas() throws Exception {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            for (int i = 0; i < 129; i++) {
                packDeltaFrame(packer, "Alice " + i);
            }
            body = packer.toByteArray();
        }

        var allFramesRead = new CountDownLatch(1);
        var unpacker = MessagePack.newDefaultUnpacker(new LatchingInputStream(body, allFramesRead));
        try (Subscription sub = new Subscription(sampleBasis(), () -> {}, unpacker)) {
            assertTrue(allFramesRead.await(5, TimeUnit.SECONDS), "reader should fill the queue");

            sub.close();

            assertTrue(sub.isDone());
            assertNull(sub.poll(5, TimeUnit.SECONDS));
        }
    }

    private static final class ThrowingInputStream extends InputStream {
        @Override
        public int read() throws IOException {
            throw new IOException("read failed");
        }
    }

    private static void packOpenFrame(MessagePacker packer) throws IOException {
        packer.packMapHeader(3);
        packer.packString("kind");
        packer.packString("open");
        packer.packString("basis");
        packBasis(packer);
        packer.packString("columns");
        packer.packArrayHeader(0);
    }

    private static void packDeltaFrame(MessagePacker packer, String name) throws IOException {
        packer.packMapHeader(3);
        packer.packString("kind");
        packer.packString("delta");
        packer.packString("basis");
        packBasis(packer);
        packer.packString("rows");
        packer.packArrayHeader(1);
        packer.packArrayHeader(2);
        packer.packArrayHeader(1);
        packer.packString(name);
        packer.packLong(1L);
    }

    private static TxBasis sampleBasis() {
        return new TxBasis(7L, Instant.ofEpochSecond(1_700_000_000L), 42L);
    }

    private static void packBasis(MessagePacker packer) throws IOException {
        packer.packMapHeader(3);
        packer.packString("tx_id");
        packer.packLong(7L);
        packer.packString("system_time");
        packer.packTimestamp(Instant.ofEpochSecond(1_700_000_000L));
        packer.packString("tx_eid");
        packer.packLong(42L);
    }

    private static final class LatchingInputStream extends InputStream {
        private final byte[] data;
        private final CountDownLatch allBytesRead;
        private int position;

        private LatchingInputStream(byte[] data, CountDownLatch allBytesRead) {
            this.data = data;
            this.allBytesRead = allBytesRead;
        }

        @Override
        public int read() {
            if (position >= data.length) {
                return -1;
            }
            int value = data[position++] & 0xff;
            if (position >= data.length) {
                allBytesRead.countDown();
            }
            return value;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (position >= data.length) {
                return -1;
            }
            int count = Math.min(len, data.length - position);
            System.arraycopy(data, position, b, off, count);
            position += count;
            if (position >= data.length) {
                allBytesRead.countDown();
            }
            return count;
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
