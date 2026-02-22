package io.triplox.client;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.triplox.client.MessageTypes.*;
import static org.junit.jupiter.api.Assertions.*;

class WireCodecTest {

    /**
     * Encode a frontend message using WireCodec write methods,
     * then decode as a backend message to test framing.
     * Since WireCodec only reads backend messages, we test by
     * writing backend messages manually and reading them back.
     */
    private BackendMessage writeAndReadBack(BackendMessageWriter writer) throws IOException {
        var baos = new ByteArrayOutputStream();
        writer.write(baos);
        var bin = new ByteArrayInputStream(baos.toByteArray());
        return WireCodec.readBackendMessage(bin);
    }

    @FunctionalInterface
    interface BackendMessageWriter {
        void write(OutputStream out) throws IOException;
    }

    // ---------------------------------------------------------------
    // Test backend message reading via manual wire encoding
    // ---------------------------------------------------------------

    private void writeBackendMessage(OutputStream out, byte type, byte[] payload) throws IOException {
        var dos = new DataOutputStream(out);
        dos.writeByte(type);
        dos.writeInt(payload.length + 4);
        dos.write(payload);
        dos.flush();
    }

    @Test
    void testAuthenticationOk() throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataTypeCodec.encodeString(dos, "triplox 0.1.0");
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_AUTHENTICATION_OK, baos.toByteArray());

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        assertInstanceOf(BackendMessage.AuthenticationOk.class, msg);
        assertEquals("triplox 0.1.0", ((BackendMessage.AuthenticationOk) msg).serverVersion());
    }

    @Test
    void testDbOpened() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeInt(5);
        dos.writeLong(42);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_DB_OPENED, payload.toByteArray());

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        var opened = (BackendMessage.DbOpened) msg;
        assertEquals(5, opened.dbId());
        assertEquals(42, opened.txId());
    }

    @Test
    void testDbClosed() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeInt(5);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_DB_CLOSED, payload.toByteArray());

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(5, ((BackendMessage.DbClosed) msg).dbId());
    }

    @Test
    void testReadyForQuery() throws IOException {
        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_READY_FOR_QUERY, new byte[]{STATUS_IDLE});

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(STATUS_IDLE, ((BackendMessage.ReadyForQuery) msg).status());
    }

    @Test
    void testRowDescription() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeInt(2); // 2 columns
        DataTypeCodec.encodeString(dos, "?e");
        dos.writeByte(TAG_LONG);
        DataTypeCodec.encodeString(dos, "?name");
        dos.writeByte(TAG_STRING);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_ROW_DESCRIPTION, payload.toByteArray());

        var msg = (BackendMessage.RowDescription) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(2, msg.columns().size());
        assertEquals("?e", msg.columns().get(0).name());
        assertEquals(TAG_LONG, msg.columns().get(0).dataType());
        assertEquals("?name", msg.columns().get(1).name());
        assertEquals(TAG_STRING, msg.columns().get(1).dataType());
    }

    @Test
    void testDataRow() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeInt(2); // 2 values
        DataTypeCodec.encode(dos, 1L);
        DataTypeCodec.encode(dos, "alice");
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_DATA_ROW, payload.toByteArray());

        var msg = (BackendMessage.DataRow) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(2, msg.values().size());
        assertEquals(1L, msg.values().get(0));
        assertEquals("alice", msg.values().get(1));
    }

    @Test
    void testCommandComplete() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        DataTypeCodec.encodeString(dos, "SELECT");
        dos.writeLong(42);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_COMMAND_COMPLETE, payload.toByteArray());

        var msg = (BackendMessage.CommandComplete) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals("SELECT", msg.tag());
        assertEquals(42, msg.rowCount());
    }

    @Test
    void testTxKey() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_TX_KEY, payload.toByteArray());

        var msg = (BackendMessage.TxKey) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(42, msg.txId());
        assertEquals(1700000000000000L, msg.systemTime());
    }

    @Test
    void testTxResultCommitted() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeByte(0); // committed
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        dos.writeLong(7);
        DataTypeCodec.encodeOptionalString(dos, null);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_TX_RESULT, payload.toByteArray());

        var msg = (BackendMessage.TxResult) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(0, msg.status());
        assertEquals(42, msg.txId());
        assertEquals(7, msg.seqNum());
        assertNull(msg.errorMessage());
    }

    @Test
    void testTxResultAborted() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeByte(1); // aborted
        dos.writeLong(42);
        dos.writeLong(1700000000000000L);
        dos.writeLong(0);
        DataTypeCodec.encodeOptionalString(dos, "constraint violation");
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_TX_RESULT, payload.toByteArray());

        var msg = (BackendMessage.TxResult) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(1, msg.status());
        assertEquals("constraint violation", msg.errorMessage());
    }

    @Test
    void testErrorResponse() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeByte(SEVERITY_ERROR);
        dos.writeShort(2000);
        DataTypeCodec.encodeString(dos, "parse error");
        DataTypeCodec.encodeOptionalString(dos, "unexpected token");
        DataTypeCodec.encodeOptionalString(dos, "check syntax");
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_ERROR_RESPONSE, payload.toByteArray());

        var msg = (BackendMessage.ErrorResponse) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(SEVERITY_ERROR, msg.severity());
        assertEquals(2000, msg.code());
        assertEquals("parse error", msg.message());
        assertEquals("unexpected token", msg.detail());
        assertEquals("check syntax", msg.hint());
    }

    @Test
    void testHeartbeat() throws IOException {
        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_HEARTBEAT, new byte[0]);

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        assertInstanceOf(BackendMessage.Heartbeat.class, msg);
    }

    @Test
    void testUnsubscribeComplete() throws IOException {
        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_UNSUBSCRIBE_COMPLETE, new byte[0]);

        var msg = WireCodec.readBackendMessage(new ByteArrayInputStream(frame.toByteArray()));
        assertInstanceOf(BackendMessage.UnsubscribeComplete.class, msg);
    }

    @Test
    void testDataBatchComplete() throws IOException {
        var payload = new ByteArrayOutputStream();
        var dos = new DataOutputStream(payload);
        dos.writeLong(100);
        dos.flush();

        var frame = new ByteArrayOutputStream();
        writeBackendMessage(frame, MSG_DATA_BATCH_COMPLETE, payload.toByteArray());

        var msg = (BackendMessage.DataBatchComplete) WireCodec.readBackendMessage(
                new ByteArrayInputStream(frame.toByteArray()));
        assertEquals(100, msg.txId());
    }

    // ---------------------------------------------------------------
    // Test frontend write methods produce valid framing
    // ---------------------------------------------------------------

    @Test
    void testStartupFraming() throws IOException {
        var baos = new ByteArrayOutputStream();
        WireCodec.writeStartup(baos, Map.of("client_name", "test"));

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        int length = dis.readInt();
        assertTrue(length >= 4 + 4 + 4); // at least version(4) + map count(4) + length itself
        short major = dis.readShort();
        short minor = dis.readShort();
        assertEquals(0, major);
        assertEquals(1, minor);
    }

    @Test
    void testTerminateFraming() throws IOException {
        var baos = new ByteArrayOutputStream();
        WireCodec.writeTerminate(baos);

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte type = dis.readByte();
        int length = dis.readInt();
        assertEquals(MSG_TERMINATE, type);
        assertEquals(4, length);
    }

    @Test
    void testQueryFraming() throws IOException {
        var baos = new ByteArrayOutputStream();
        WireCodec.writeQuery(baos, "{:find [?e]}", 1);

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte type = dis.readByte();
        int length = dis.readInt();
        assertEquals(MSG_QUERY, type);
        assertTrue(length > 4);

        // Decode the payload
        String queryStr = DataTypeCodec.decodeString(dis);
        int dbId = dis.readInt();
        assertEquals("{:find [?e]}", queryStr);
        assertEquals(1, dbId);
    }

    @Test
    void testExecuteFraming() throws IOException {
        var doc = new TreeMap<String, Object>();
        doc.put("db/id", 1L);
        var ops = List.<TxOp>of(new TxOp.Put(doc));

        var baos = new ByteArrayOutputStream();
        WireCodec.writeExecute(baos, ops, true);

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        byte type = dis.readByte();
        int length = dis.readInt();
        assertEquals(MSG_EXECUTE, type);
        assertTrue(length > 4);
    }

    @Test
    void testOpenDbFraming() throws IOException {
        var baos = new ByteArrayOutputStream();
        WireCodec.writeOpenDb(baos, null);

        var dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(MSG_OPEN_DB, dis.readByte());
        int length = dis.readInt();
        assertEquals(5, length); // 4 + 1 byte for None tag

        baos = new ByteArrayOutputStream();
        WireCodec.writeOpenDb(baos, 42L);

        dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        assertEquals(MSG_OPEN_DB, dis.readByte());
        length = dis.readInt();
        assertEquals(13, length); // 4 + 1 byte tag + 8 byte i64
    }
}
