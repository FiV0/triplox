package xyz.triplox.client;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class SubscriptionTest {

    @Test
    void clientDisablesReadTimeoutForLongRunningRequests() {
        var client = TriploxNode.createHttpClient();

        assertEquals(0, client.readTimeoutMillis());
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
        try (Subscription sub = new Subscription(first.txKey(), () -> {}, unpacker)) {
            var ex = assertThrows(IllegalStateException.class, () -> sub.poll(5, TimeUnit.SECONDS));
            assertTrue(ex.getMessage().contains("unexpected open frame mid-stream"));
        }
    }

    @Test
    void terminalErrorPreservesAlreadyQueuedDeltas() throws Exception {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packDeltaFrame(packer, "Alice");
            packErrorFrame(packer);
            body = packer.toByteArray();
        }

        var unpacker = MessagePack.newDefaultUnpacker(new ByteArrayInputStream(body));
        try (Subscription sub = new Subscription(sampleBasis(), () -> {}, unpacker)) {
            Thread.sleep(100);

            var delta = sub.poll(5, TimeUnit.SECONDS);
            assertNotNull(delta, "expected queued delta before terminal error");
            assertEquals("Alice", delta.rows().getFirst().values().getFirst());

            var ex = assertThrows(TriploxException.class, () -> sub.poll(5, TimeUnit.SECONDS));
            assertEquals(4000, Short.toUnsignedInt(ex.code()));
            assertTrue(ex.getMessage().contains("boom"));
        }
    }

    @Test
    void takeReturnsNullOnStreamEof() throws Exception {
        var unpacker = MessagePack.newDefaultUnpacker(new ByteArrayInputStream(new byte[0]));
        try (Subscription sub = new Subscription(sampleBasis(), () -> {}, unpacker)) {
            var delta = assertTimeoutPreemptively(Duration.ofSeconds(5), sub::take);

            assertNull(delta);
            assertTrue(sub.isDone());
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

        var unpacker = MessagePack.newDefaultUnpacker(new ByteArrayInputStream(body));
        try (Subscription sub = new Subscription(sampleBasis(), () -> {}, unpacker)) {
            assertNotNull(sub.poll(5, TimeUnit.SECONDS), "expected a queued delta");

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
        packer.packString("tx_key");
        packBasis(packer);
        packer.packString("columns");
        packer.packArrayHeader(0);
    }

    private static void packDeltaFrame(MessagePacker packer, String name) throws IOException {
        packer.packMapHeader(3);
        packer.packString("kind");
        packer.packString("delta");
        packer.packString("tx_key");
        packBasis(packer);
        packer.packString("rows");
        packer.packArrayHeader(1);
        packer.packArrayHeader(2);
        packer.packArrayHeader(1);
        packer.packString(name);
        packer.packLong(1L);
    }

    private static void packErrorFrame(MessagePacker packer) throws IOException {
        packer.packMapHeader(6);
        packer.packString("kind");
        packer.packString("error");
        packer.packString("severity");
        packer.packString("E");
        packer.packString("code");
        packer.packLong(4000L);
        packer.packString("message");
        packer.packString("boom");
        packer.packString("detail");
        packer.packNil();
        packer.packString("hint");
        packer.packNil();
    }

    private static TxKey sampleBasis() {
        return new TxKey(7L, Instant.ofEpochSecond(1_700_000_000L));
    }

    private static void packBasis(MessagePacker packer) throws IOException {
        packer.packMapHeader(2);
        packer.packString("tx_id");
        packer.packLong(7L);
        packer.packString("system_time");
        packer.packTimestamp(Instant.ofEpochSecond(1_700_000_000L));
    }

}
