package io.triplox.client;

import clojure.lang.Keyword;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;
import java.util.TreeMap;

import static io.triplox.client.MessageTypes.*;
import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    // ---------------------------------------------------------------
    // Request body encoding tests
    // ---------------------------------------------------------------

    @Test
    void testEncodeOpenDbBodyNull() throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(null);
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        // Two None tags (tx_id, system_time)
        assertEquals(0x00, dis.readByte());
        assertEquals(0x00, dis.readByte());
        assertEquals(0, dis.available());
    }

    @Test
    void testEncodeOpenDbBodyWithTxId() throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(42L);
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        // Some(42) for tx_id
        assertEquals(0x01, dis.readByte());
        assertEquals(42L, dis.readLong());
        // None for system_time
        assertEquals(0x00, dis.readByte());
        assertEquals(0, dis.available());
    }

    @Test
    void testEncodeQueryBody() throws IOException {
        byte[] body = WireCodec.encodeQueryBody("{:find [?e]}", List.of());
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        String query = DataTypeCodec.decodeString(dis);
        assertEquals("{:find [?e]}", query);
        int argsCount = dis.readInt();
        assertEquals(0, argsCount);
    }

    @Test
    void testEncodeExecuteBody() throws IOException {
        var doc = new TreeMap<Keyword, Object>();
        doc.put(Keyword.intern("db", "id"), 1L);
        var ops = List.<TxOp>of(new TxOp.Put(doc));

        byte[] body = WireCodec.encodeExecuteBody(ops);
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        int count = dis.readInt();
        assertEquals(1, count);
        // First byte is the op type tag
        assertEquals(TXOP_PUT, dis.readByte());
    }

    // ---------------------------------------------------------------
    // Response body decoding tests
    // ---------------------------------------------------------------

    @Test
    void testDecodeDbOpened() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeInt(5);
        dos.writeLong(42);
        dos.flush();

        var opened = WireCodec.decodeDbOpened(baos.toByteArray());
        assertEquals(5, opened.dbId());
        assertEquals(42, opened.txId());
    }

    @Test
    void testDecodeDbClosed() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeInt(5);
        dos.flush();

        var closed = WireCodec.decodeDbClosed(baos.toByteArray());
        assertEquals(5, closed.dbId());
    }

    @Test
    void testDecodeTxKey() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        dos.flush();

        var txKey = WireCodec.decodeTxKey(baos.toByteArray());
        assertEquals(42, txKey.txId());
        assertEquals(1700000000000000L, txKey.systemTime());
    }

    @Test
    void testDecodeTxResultCommitted() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeByte(0);
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        DataTypeCodec.encodeOptionalString(dos, null);
        dos.flush();

        var result = WireCodec.decodeTxResult(baos.toByteArray());
        assertEquals(0, result.status());
        assertEquals(42, result.txId());
        assertNull(result.errorMessage());
    }

    @Test
    void testDecodeTxResultAborted() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeByte(1);
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        DataTypeCodec.encodeOptionalString(dos, "constraint violation");
        dos.flush();

        var result = WireCodec.decodeTxResult(baos.toByteArray());
        assertEquals(1, result.status());
        assertEquals("constraint violation", result.errorMessage());
    }

    @Test
    void testDecodeErrorResponse() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeByte(SEVERITY_ERROR);
        dos.writeShort(2000);
        DataTypeCodec.encodeString(dos, "parse error");
        DataTypeCodec.encodeOptionalString(dos, "unexpected token");
        DataTypeCodec.encodeOptionalString(dos, "check syntax");
        dos.flush();

        var err = WireCodec.decodeErrorResponse(baos.toByteArray());
        assertEquals(SEVERITY_ERROR, err.severity());
        assertEquals(2000, err.code());
        assertEquals("parse error", err.message());
        assertEquals("unexpected token", err.detail());
        assertEquals("check syntax", err.hint());
    }

    @Test
    void testDecodeQueryResponse() throws IOException {
        // Build a query response: RowDescription + 2 DataRows
        var response = new ByteArrayOutputStream();

        // RowDescription frame
        var rowDescPayload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(rowDescPayload);
        dos.writeInt(2); // 2 columns
        DataTypeCodec.encodeString(dos, "?e");
        dos.writeByte(TAG_LONG);
        DataTypeCodec.encodeString(dos, "?name");
        dos.writeByte(TAG_STRING);
        dos.flush();
        writeFrame(response, MSG_ROW_DESCRIPTION, rowDescPayload.toByteArray());

        // DataRow 1
        var row1Payload = new ByteArrayOutputStream();
        dos = new DataOutputStream(row1Payload);
        dos.writeInt(2); // 2 values
        DataTypeCodec.encode(dos, 1L);
        DataTypeCodec.encode(dos, "alice");
        dos.flush();
        writeFrame(response, MSG_DATA_ROW, row1Payload.toByteArray());

        // DataRow 2
        var row2Payload = new ByteArrayOutputStream();
        dos = new DataOutputStream(row2Payload);
        dos.writeInt(2);
        DataTypeCodec.encode(dos, 2L);
        DataTypeCodec.encode(dos, "bob");
        dos.flush();
        writeFrame(response, MSG_DATA_ROW, row2Payload.toByteArray());

        var result = WireCodec.decodeQueryResponse(response.toByteArray());
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
        var response = new ByteArrayOutputStream();

        // RowDescription with 1 column, no DataRows
        var rowDescPayload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(rowDescPayload);
        dos.writeInt(1);
        DataTypeCodec.encodeString(dos, "?e");
        dos.writeByte(TAG_LONG);
        dos.flush();
        writeFrame(response, MSG_ROW_DESCRIPTION, rowDescPayload.toByteArray());

        var result = WireCodec.decodeQueryResponse(response.toByteArray());
        assertEquals(1, result.columns().size());
        assertTrue(result.rows().isEmpty());
    }

    // ---------------------------------------------------------------
    // Helper
    // ---------------------------------------------------------------

    private void writeFrame(OutputStream out, byte type, byte[] payload) throws IOException {
        var dos = new DataOutputStream(out);
        dos.writeByte(type);
        dos.writeInt(payload.length + 4);
        dos.write(payload);
        dos.flush();
    }
}
