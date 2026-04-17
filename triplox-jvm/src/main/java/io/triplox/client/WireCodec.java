package io.triplox.client;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import static io.triplox.client.MessageTypes.*;

/**
 * Codec for encoding/decoding Triplox binary payloads for HTTP transport.
 *
 * Produces and consumes raw byte arrays suitable for HTTP request/response bodies.
 * The binary encoding of values (DataType, TxOp, QueryArg) is unchanged from
 * the TCP wire protocol — only the framing layer is replaced by HTTP.
 */
public final class WireCodec {
    private WireCodec() {}

    // ---------------------------------------------------------------
    // Request body encoding (client → server)
    // ---------------------------------------------------------------

    /**
     * Encode an OpenDb request body: option_i64(txId) + option_i64(null).
     */
    public static byte[] encodeOpenDbBody(Long txId) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataTypeCodec.encodeOptionalLong(dos, txId);   // tx_id
        DataTypeCodec.encodeOptionalLong(dos, null);   // system_time (always null from client)
        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Encode a Query request body: string(query) + query_args(args).
     */
    public static byte[] encodeQueryBody(String query, List<QueryArg> args) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataTypeCodec.encodeString(dos, query);
        QueryArg.encodeArgs(dos, args);
        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Encode an Execute request body: tx_ops(ops).
     */
    public static byte[] encodeExecuteBody(List<TxOp> ops) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        TxOpCodec.encode(dos, ops);
        dos.flush();
        return baos.toByteArray();
    }

    // ---------------------------------------------------------------
    // Response body decoding (server → client)
    // ---------------------------------------------------------------

    /**
     * Decode a DbOpened response body: u32(db_id) + i64(tx_id).
     */
    public static BackendMessage.DbOpened decodeDbOpened(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        return new BackendMessage.DbOpened(dis.readInt(), dis.readLong());
    }

    /**
     * Decode a DbClosed response body: u32(db_id).
     */
    public static BackendMessage.DbClosed decodeDbClosed(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        return new BackendMessage.DbClosed(dis.readInt());
    }

    /**
     * Decode a query response from concatenated framed backend messages.
     * Format: [type:1][length:4][payload]* where messages are RowDescription + DataRows.
     */
    public static QueryResult decodeQueryResponse(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        List<ColumnDesc> columns = null;
        var rows = new ArrayList<List<Object>>();

        while (dis.available() > 0) {
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
            switch (type) {
                case MSG_ROW_DESCRIPTION -> {
                    int count = pin.readInt();
                    columns = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        columns.add(new ColumnDesc(DataTypeCodec.decodeString(pin), pin.readByte()));
                    }
                }
                case MSG_DATA_ROW -> {
                    int count = pin.readInt();
                    var values = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        values.add(DataTypeCodec.decode(pin));
                    }
                    rows.add(values);
                }
                default -> throw new IOException("Unexpected message type in query response: 0x" +
                        Integer.toHexString(type & 0xFF));
            }
        }

        if (columns == null) {
            throw new IOException("Missing RowDescription in query response");
        }
        return new QueryResult(columns, rows);
    }

    /**
     * Decode a TxKey response body: i64(tx_id) + i64(system_time).
     */
    public static BackendMessage.TxKey decodeTxKey(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        return new BackendMessage.TxKey(dis.readLong(), dis.readLong());
    }

    /**
     * Decode a TxResult response body: u8(status) + i64(tx_id) + i64(system_time) + option_string(error).
     */
    public static BackendMessage.TxResult decodeTxResult(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        return new BackendMessage.TxResult(
                dis.readByte(), dis.readLong(), dis.readLong(),
                DataTypeCodec.decodeOptionalString(dis));
    }

    /**
     * Decode an ErrorResponse body: u8(severity) + u16(code) + string(message) + option(detail) + option(hint).
     */
    public static BackendMessage.ErrorResponse decodeErrorResponse(byte[] body) throws IOException {
        var dis = new DataInputStream(new ByteArrayInputStream(body));
        return new BackendMessage.ErrorResponse(
                dis.readByte(), dis.readShort(), DataTypeCodec.decodeString(dis),
                DataTypeCodec.decodeOptionalString(dis), DataTypeCodec.decodeOptionalString(dis));
    }
}
