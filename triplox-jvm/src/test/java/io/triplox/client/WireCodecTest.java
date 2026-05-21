package io.triplox.client;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.TreeMap;

import static io.triplox.client.MessageTypes.*;
import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    // ---------------------------------------------------------------
    // Request body encoding
    // ---------------------------------------------------------------

    @Test
    void testEncodeOpenDbBodyNull() throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(null);
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(3, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            unpacker.unpackNil();
            assertEquals("system_time", unpacker.unpackString());
            unpacker.unpackNil();
            assertEquals("tx_eid", unpacker.unpackString());
            unpacker.unpackNil();
            assertFalse(unpacker.hasNext());
        }
    }

    @Test
    void testEncodeOpenDbBodyWithTxId() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        byte[] body = WireCodec.encodeOpenDbBody(new TxBasis(42L, now, 100L));
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(3, unpacker.unpackMapHeader());
            assertEquals("tx_id", unpacker.unpackString());
            assertEquals(42L, unpacker.unpackLong());
            assertEquals("system_time", unpacker.unpackString());
            assertEquals(now, unpacker.unpackTimestamp());
            assertEquals("tx_eid", unpacker.unpackString());
            assertEquals(100L, unpacker.unpackLong());
        }
    }

    @Test
    void testEncodeOpenDbBodyRejectsPartialTxKey() {
        assertThrows(IOException.class, () -> WireCodec.encodeOpenDbBody(42L, null, null));
        assertThrows(IOException.class, () -> WireCodec.encodeOpenDbBody(null, Instant.EPOCH, null));
        assertThrows(IOException.class, () -> WireCodec.encodeOpenDbBody(42L, Instant.EPOCH, null));
    }

    @Test
    void testEncodeQueryBody() throws IOException {
        byte[] body = WireCodec.encodeQueryBody("{:find [?e]}", List.of());
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            assertEquals(2, unpacker.unpackMapHeader());
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
    void testDecodeDbOpened() throws IOException {
        byte[] body = packMap2("db_id", 5L, "tx_eid", 42L);
        var opened = WireCodec.decodeDbOpened(body);
        assertEquals(5, opened.dbId());
        assertEquals(42L, opened.txEid());
    }

    @Test
    void testDecodeDbClosed() throws IOException {
        byte[] body = packMap1("db_id", 5L);
        var closed = WireCodec.decodeDbClosed(body);
        assertEquals(5, closed.dbId());
    }

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
            packer.packMapHeader(5);
            packer.packString("status"); packer.packLong(0);
            packer.packString("tx_id"); packer.packLong(42);
            packer.packString("system_time"); packer.packTimestamp(now);
            packer.packString("tx_eid"); packer.packLong(100);
            packer.packString("error_message"); packer.packNil();
            body = packer.toByteArray();
        }
        var result = WireCodec.decodeTxResult(body);
        assertEquals((byte) 0, result.status());
        assertEquals(42L, result.txId());
        assertEquals(100L, result.txEid());
        assertEquals(now, result.systemTime());
        assertNull(result.errorMessage());
    }

    @Test
    void testDecodeTxResultAborted() throws IOException {
        Instant now = Instant.ofEpochSecond(1_700_000_000L);
        byte[] body;
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(5);
            packer.packString("status"); packer.packLong(1);
            packer.packString("tx_id"); packer.packLong(42);
            packer.packString("system_time"); packer.packTimestamp(now);
            packer.packString("tx_eid"); packer.packLong(101);
            packer.packString("error_message"); packer.packString("constraint violation");
            body = packer.toByteArray();
        }
        var result = WireCodec.decodeTxResult(body);
        assertEquals((byte) 1, result.status());
        assertEquals(101L, result.txEid());
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
    // Helpers
    // ---------------------------------------------------------------

    private static void packColumn(MessagePacker packer, String name, byte type) throws IOException {
        packer.packMapHeader(2);
        packer.packString("name"); packer.packString(name);
        packer.packString("type"); packer.packLong(Byte.toUnsignedLong(type));
    }

    private static byte[] packMap1(String k1, long v1) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(1);
            packer.packString(k1); packer.packLong(v1);
            return packer.toByteArray();
        }
    }

    private static byte[] packMap2(String k1, long v1, String k2, long v2) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(2);
            packer.packString(k1); packer.packLong(v1);
            packer.packString(k2); packer.packLong(v2);
            return packer.toByteArray();
        }
    }
}
