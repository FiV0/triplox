package xyz.triplox.client;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.TreeMap;

import static xyz.triplox.client.MessageTypes.*;
import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    // ---------------------------------------------------------------
    // Request body encoding
    // ---------------------------------------------------------------

    @Test
    void testEncodeOpenDbBodyNull() throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(null);
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(2, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            unpacker.unpackNil();
            assertEquals("system_time", unpacker.unpackString());
            unpacker.unpackNil();
            assertFalse(unpacker.hasNext());
        }
    }

    @Test
    void testEncodeOpenDbBodyWithTxId() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        byte[] body = WireCodec.encodeOpenDbBody(new TxKey(42L, now));
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(2, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            assertEquals(42L, unpacker.unpackLong());
            assertEquals("system_time", unpacker.unpackString());
            assertEquals(now, unpacker.unpackTimestamp());
        }
    }

    @Test
    void testEncodeOpenDbBodyRejectsPartialTxKey() {
        assertThrows(IOException.class, () -> WireCodec.encodeOpenDbBody(42L, null));
        assertThrows(IOException.class, () -> WireCodec.encodeOpenDbBody(null, Instant.EPOCH));
    }

    @Test
    void testEncodeQueryBody() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body = WireCodec.encodeQueryBody(new TxKey(42L, now), "{:find [?e]}", List.of());
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(3, unpacker.unpackMapHeader());
            assertEquals("tx_key", unpacker.unpackString());
            assertEquals(2, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            assertEquals(42L, unpacker.unpackLong());
            assertEquals("system_time", unpacker.unpackString());
            assertEquals(now, unpacker.unpackTimestamp());
            assertEquals("query", unpacker.unpackString());
            assertEquals("{:find [?e]}", unpacker.unpackString());
            assertEquals("args", unpacker.unpackString());
            assertEquals(0, unpacker.unpackArrayHeader());
        }
    }

    @Test
    void testEncodeExecuteBody() throws IOException {
        var doc = new TreeMap<String, Object>();
        doc.put(":db/id", 1L);
        var ops = List.<TxOp>of(new TxOp.Put(doc));

        byte[] body = WireCodec.encodeExecuteBody(ops);
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(1, unpacker.unpackMapHeader());
            assertEquals("ops", unpacker.unpackString());
            var decoded = TxOpCodec.unpackOps(unpacker);
            assertEquals(1, decoded.size());
            assertInstanceOf(TxOp.Put.class, decoded.get(0));
        }
    }

    // ---------------------------------------------------------------
    // Response body decoding
    // ---------------------------------------------------------------

    @Test
    void testDecodeTxKey() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(2);
            packer.packString("tx_id"); packer.packLong(42);
            packer.packString("system_time"); packer.packTimestamp(now);
            body = packer.toByteArray();
        }
        var txKey = WireCodec.decodeTxKey(body);
        assertEquals(42L, txKey.txId());
        assertEquals(now, txKey.systemTime());
    }

    @Test
    void testDecodeTxResultCommitted() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(4);
            packer.packString("status"); packer.packLong(0);
            packer.packString("tx_id"); packer.packLong(42);
            packer.packString("system_time"); packer.packTimestamp(now);
            packer.packString("error_message"); packer.packNil();
            body = packer.toByteArray();
        }
        var result = WireCodec.decodeTxResult(body);
        assertEquals((byte) 0, result.status());
        assertEquals(42L, result.txId());
        assertEquals(now, result.systemTime());
        assertNull(result.errorMessage());
    }

    @Test
    void testDecodeTxResultAborted() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(4);
            packer.packString("status"); packer.packLong(1);
            packer.packString("tx_id"); packer.packLong(42);
            packer.packString("system_time"); packer.packTimestamp(now);
            packer.packString("error_message"); packer.packString("constraint violation");
            body = packer.toByteArray();
        }
        var result = WireCodec.decodeTxResult(body);
        assertEquals((byte) 1, result.status());
        assertEquals("constraint violation", result.errorMessage());
    }

    @Test
    void testDecodeErrorResponse() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(5);
            packer.packString("severity"); packer.packString("E");
            packer.packString("code"); packer.packLong(2000);
            packer.packString("message"); packer.packString("parse error");
            packer.packString("detail"); packer.packString("unexpected token");
            packer.packString("hint"); packer.packString("check syntax");
            body = packer.toByteArray();
        }
        var err = WireCodec.decodeErrorResponse(body);
        assertEquals(SEVERITY_ERROR, err.severity());
        assertEquals((short) 2000, err.code());
        assertEquals("parse error", err.message());
        assertEquals("unexpected token", err.detail());
        assertEquals("check syntax", err.hint());
    }

    @Test
    void testDecodeQueryResponse() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(2);
            packer.packString("columns");
            packer.packArrayHeader(2);
            packColumn(packer, "?e", TAG_LONG);
            packColumn(packer, "?name", TAG_STRING);
            packer.packString("rows");
            packer.packArrayHeader(2);
            packer.packArrayHeader(2);
            packer.packLong(1); packer.packString("alice");
            packer.packArrayHeader(2);
            packer.packLong(2); packer.packString("bob");
            body = packer.toByteArray();
        }

        var result = WireCodec.decodeQueryResponse(body);
        assertEquals(2, result.columns().size());
        assertEquals("?e", result.columns().get(0).name());
        assertEquals(TAG_LONG, result.columns().get(0).dataType());
        assertEquals(2, result.rows().size());
        assertEquals(1L, result.rows().get(0).get(0));
        assertEquals("alice", result.rows().get(0).get(1));
        assertEquals(2L, result.rows().get(1).get(0));
        assertEquals("bob", result.rows().get(1).get(1));
    }

    @Test
    void testDecodeQueryResponseEmpty() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(2);
            packer.packString("columns");
            packer.packArrayHeader(1);
            packColumn(packer, "?e", TAG_LONG);
            packer.packString("rows");
            packer.packArrayHeader(0);
            body = packer.toByteArray();
        }
        var result = WireCodec.decodeQueryResponse(body);
        assertEquals(1, result.columns().size());
        assertTrue(result.rows().isEmpty());
    }

    // ---------------------------------------------------------------
    // Subscription frames
    // ---------------------------------------------------------------

    @Test
    void testEncodeSubscribeBodyNullDb() throws IOException {
        byte[] body = WireCodec.encodeSubscribeBody(null, "[:find ?n :where [?e :name ?n]]", List.of());
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(3, unpacker.unpackMapHeader());
            assertEquals("tx_key", unpacker.unpackString());
            unpacker.unpackNil();
            assertEquals("query", unpacker.unpackString());
            assertEquals("[:find ?n :where [?e :name ?n]]", unpacker.unpackString());
            assertEquals("args", unpacker.unpackString());
            assertEquals(0, unpacker.unpackArrayHeader());
        }
    }

    @Test
    void testEncodeSubscribeBodyWithDb() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body = WireCodec.encodeSubscribeBody(new TxKey(7L, now), "[:find ?n]", List.of());
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(3, unpacker.unpackMapHeader());
            assertEquals("tx_key", unpacker.unpackString());
            assertEquals(2, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            assertEquals(7L, unpacker.unpackLong());
            assertEquals("system_time", unpacker.unpackString());
            assertEquals(now, unpacker.unpackTimestamp());
        }
    }

    @Test
    void testDecodeOpenFrame() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("kind"); packer.packString("open");
            packer.packString("tx_key"); WireCodec.packTxKey(packer, new TxKey(7L, now));
            packer.packString("columns");
            packer.packArrayHeader(1);
            packColumn(packer, "?name", (byte) 255);
            body = packer.toByteArray();
        }
        var open = assertInstanceOf(SubscriptionFrame.Open.class, decodeFrame(body));
        assertEquals(new TxKey(7L, now), open.txKey());
        assertEquals(1, open.columns().size());
        assertEquals("?name", open.columns().get(0).name());
    }

    @Test
    void testDecodeDeltaFrameWithSignedWeights() throws IOException {
        byte[] body;
        var now = Instant.now();
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("kind"); packer.packString("delta");
            packer.packString("tx_key"); WireCodec.packTxKey(packer, new TxKey(7L, now));
            packer.packString("rows");
            packer.packArrayHeader(2);
            packDeltaRow(packer, "Ivan", 1);
            packDeltaRow(packer, "Petr", -2);
            body = packer.toByteArray();
        }
        var delta = assertInstanceOf(Delta.class, decodeFrame(body));
        assertEquals(new TxKey(7L, now), delta.txKey());
        assertEquals(2, delta.rows().size());
        assertEquals(List.of("Ivan"), delta.rows().get(0).values());
        assertEquals(1L, delta.rows().get(0).weight());
        assertEquals(List.of("Petr"), delta.rows().get(1).values());
        assertEquals(-2L, delta.rows().get(1).weight());
    }

    @Test
    void testDecodeDeltaFrameRejectsNilBasis() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("kind"); packer.packString("delta");
            packer.packString("tx_key"); packer.packNil();
            packer.packString("rows");
            packer.packArrayHeader(0);
            body = packer.toByteArray();
        }
        var err = assertThrows(IOException.class, () -> decodeFrame(body));
        assertTrue(err.getMessage().contains("tx_key cannot be nil"));
    }

    @Test
    void testDecodeErrorFrame() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(6);
            packer.packString("kind"); packer.packString("error");
            packer.packString("severity"); packer.packString("F");
            packer.packString("code"); packer.packLong(4000);
            packer.packString("message"); packer.packString("boom");
            packer.packString("detail"); packer.packNil();
            packer.packString("hint"); packer.packNil();
            body = packer.toByteArray();
        }
        var err = assertInstanceOf(SubscriptionFrame.Error.class, decodeFrame(body));
        assertEquals(SEVERITY_FATAL, err.error().severity());
        assertEquals((short) 4000, err.error().code());
        assertEquals("boom", err.error().message());
    }

    @Test
    void testDecodeUnknownFrame() throws IOException {
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(1);
            packer.packString("kind"); packer.packString("heartbeat");
            body = packer.toByteArray();
        }
        var ex = assertThrows(IOException.class, () -> decodeFrame(body));
        assertTrue(ex.getMessage().contains("unknown subscription frame kind: heartbeat"));
    }

    @Test
    void testDecodeDeltaFrameKeyOrderIndependent() throws IOException {
        byte[] body;
        var now = Instant.now();
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("rows");
            packer.packArrayHeader(1);
            packDeltaRow(packer, "Ann", 1);
            packer.packString("kind"); packer.packString("delta");
            packer.packString("tx_key"); WireCodec.packTxKey(packer, new TxKey(3L, now));
            body = packer.toByteArray();
        }
        var delta = assertInstanceOf(Delta.class, decodeFrame(body));
        assertEquals(new TxKey(3L, now), delta.txKey());
        assertEquals(List.of("Ann"), delta.rows().get(0).values());
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    private static void packColumn(MessagePacker packer, String name, byte type) throws IOException {
        packer.packMapHeader(2);
        packer.packString("name"); packer.packString(name);
        packer.packString("type"); packer.packLong(Byte.toUnsignedLong(type));
    }

    private static SubscriptionFrame decodeFrame(byte[] body) throws IOException {
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            return WireCodec.decodeSubscriptionFrame(unpacker);
        }
    }


    private static void packDeltaRow(MessagePacker packer, String value, long weight) throws IOException {
        packer.packArrayHeader(2);
        packer.packArrayHeader(1); packer.packString(value);
        packer.packLong(weight);
    }

}
