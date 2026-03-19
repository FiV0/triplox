package io.triplox.client;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.triplox.client.MessageTypes.*;

/**
 * Wire-level codec for framing Triplox protocol messages.
 *
 * Frontend (write) methods build the payload into a buffer first
 * to compute the length, then write the framed message.
 *
 * The read method reads a single backend message from the stream.
 */
public final class WireCodec {
    private WireCodec() {}

    // ---------------------------------------------------------------
    // Write frontend messages
    // ---------------------------------------------------------------

    /**
     * Startup message — special: no type byte, just [length:u32][payload].
     */
    public static void writeStartup(OutputStream out, Map<String, String> params) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        dos.writeShort(PROTOCOL_VERSION_MAJOR);
        dos.writeShort(PROTOCOL_VERSION_MINOR);
        var sorted = new TreeMap<>(params);
        DataTypeCodec.encodeStringMap(dos, sorted);
        dos.flush();
        byte[] payload = baos.toByteArray();

        var frame = new DataOutputStream(out);
        frame.writeInt(payload.length + 4); // length includes itself
        frame.write(payload);
        frame.flush();
    }

    public static void writeOpenDb(OutputStream out, Long basisTxId) throws IOException {
        writeFramed(out, MSG_OPEN_DB, dos -> {
            DataTypeCodec.encodeOptionalLong(dos, basisTxId);  // basis_tx_id
            DataTypeCodec.encodeOptionalLong(dos, null);       // basis_system_time
        });
    }

    public static void writeCloseDb(OutputStream out, int dbId) throws IOException {
        writeFramed(out, MSG_CLOSE_DB, dos -> dos.writeInt(dbId));
    }

    public static void writeQuery(OutputStream out, String query, int dbId) throws IOException {
        writeFramed(out, MSG_QUERY, dos -> {
            DataTypeCodec.encodeString(dos, query);
            dos.writeInt(dbId);
        });
    }

    public static void writeExecute(OutputStream out, List<TxOp> ops, boolean awaitIndexing) throws IOException {
        writeFramed(out, MSG_EXECUTE, dos -> {
            TxOpCodec.encode(dos, ops);
            dos.writeBoolean(awaitIndexing);
        });
    }

    public static void writeSubscribe(OutputStream out, String query, int dbId) throws IOException {
        writeFramed(out, MSG_SUBSCRIBE, dos -> {
            DataTypeCodec.encodeString(dos, query);
            dos.writeInt(dbId);
        });
    }

    public static void writeUnsubscribe(OutputStream out) throws IOException {
        writeEmpty(out, MSG_UNSUBSCRIBE);
    }

    public static void writeTerminate(OutputStream out) throws IOException {
        writeEmpty(out, MSG_TERMINATE);
    }

    // ---------------------------------------------------------------
    // Read backend messages
    // ---------------------------------------------------------------

    public static BackendMessage readBackendMessage(InputStream in) throws IOException {
        var dis = new DataInputStream(in);
        byte type = dis.readByte();
        int length = dis.readInt();
        if (length < 4) {
            throw new IOException("Invalid message length: " + length);
        }
        int payloadSize = length - 4;
        byte[] payload = new byte[payloadSize];
        if (payloadSize > 0) {
            dis.readFully(payload);
        }

        var pin = new DataInputStream(new ByteArrayInputStream(payload));
        return decodeBackendPayload(type, pin);
    }

    // ---------------------------------------------------------------
    // Internal: framing helpers
    // ---------------------------------------------------------------

    @FunctionalInterface
    interface PayloadWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static void writeFramed(OutputStream out, byte typeByte, PayloadWriter writer) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        writer.write(dos);
        dos.flush();
        byte[] payload = baos.toByteArray();

        var frame = new DataOutputStream(out);
        frame.writeByte(typeByte);
        frame.writeInt(payload.length + 4);
        frame.write(payload);
        frame.flush();
    }

    private static void writeEmpty(OutputStream out, byte typeByte) throws IOException {
        var frame = new DataOutputStream(out);
        frame.writeByte(typeByte);
        frame.writeInt(4);
        frame.flush();
    }

    // ---------------------------------------------------------------
    // Internal: backend payload decoding
    // ---------------------------------------------------------------

    private static BackendMessage decodeBackendPayload(byte type, DataInputStream in) throws IOException {
        return switch (type) {
            case MSG_AUTHENTICATION_OK -> new BackendMessage.AuthenticationOk(
                    DataTypeCodec.decodeString(in));

            case MSG_DB_OPENED -> new BackendMessage.DbOpened(in.readInt(), in.readLong());

            case MSG_DB_CLOSED -> new BackendMessage.DbClosed(in.readInt());

            case MSG_ROW_DESCRIPTION -> {
                int count = in.readInt();
                var columns = new ArrayList<ColumnDesc>(count);
                for (int i = 0; i < count; i++) {
                    columns.add(new ColumnDesc(DataTypeCodec.decodeString(in), in.readByte()));
                }
                yield new BackendMessage.RowDescription(columns);
            }

            case MSG_DATA_ROW -> {
                int count = in.readInt();
                var values = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    values.add(DataTypeCodec.decode(in));
                }
                yield new BackendMessage.DataRow(values);
            }

            case MSG_DATA_BATCH_COMPLETE -> new BackendMessage.DataBatchComplete(in.readLong());

            case MSG_READY_FOR_QUERY -> new BackendMessage.ReadyForQuery(in.readByte());

            case MSG_TX_KEY -> new BackendMessage.TxKey(in.readLong(), in.readLong());

            case MSG_TX_RESULT -> new BackendMessage.TxResult(
                    in.readByte(), in.readLong(), in.readLong(),
                    DataTypeCodec.decodeOptionalString(in));

            case MSG_UNSUBSCRIBE_COMPLETE -> new BackendMessage.UnsubscribeComplete();

            case MSG_HEARTBEAT -> new BackendMessage.Heartbeat();

            case MSG_ERROR_RESPONSE -> new BackendMessage.ErrorResponse(
                    in.readByte(), in.readShort(), DataTypeCodec.decodeString(in),
                    DataTypeCodec.decodeOptionalString(in), DataTypeCodec.decodeOptionalString(in));

            default -> throw new IOException("Unknown backend message type: 0x" + Integer.toHexString(type & 0xFF));
        };
    }
}
