package xyz.triplox.client;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

/**
 * MessagePack codec for the HTTP/2 request and response bodies.
 *
 * <p>Each body is a single msgpack map with string keys. See
 * {@code design/PROTOCOL.md} for the schemas.</p>
 */
public final class WireCodec {
    private WireCodec() {}

    // ---------------------------------------------------------------
    // Request bodies (client → server)
    // ---------------------------------------------------------------

    /**
     * {@code POST /db/open} body: {@code {"tx_id": int|nil, "system_time": Timestamp|nil}}.
     */
    public static byte[] encodeOpenDbBody(TxKey basis) throws IOException {
        return encodeOpenDbBody(
                basis == null ? null : basis.txId(),
                basis == null ? null : basis.systemTime());
    }

    /**
     * {@code POST /db/open} body: {@code {"tx_id": int|nil, "system_time": Timestamp|nil}}.
     */
    public static byte[] encodeOpenDbBody(Long txId, Instant systemTime) throws IOException {
        if ((txId == null) != (systemTime == null)) {
            throw new IOException("tx_id and system_time must both be set, or both be nil");
        }
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(2);
            packer.packString("tx_id");
            if (txId == null) packer.packNil();
            else packer.packLong(txId);
            packer.packString("system_time");
            if (systemTime == null) packer.packNil();
            else packer.packTimestamp(systemTime);
            return packer.toByteArray();
        }
    }

    /**
     * {@code POST /db/query} body: {@code {"basis_tx_key": TxKey, "query": str, "args": [QueryArg, ...]}}.
     */
    public static byte[] encodeQueryBody(TxKey basis, String query, List<QueryArg> args) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("basis_tx_key"); packTxKey(packer, basis);
            packer.packString("query"); packer.packString(query);
            packer.packString("args"); QueryArg.packAll(packer, args);
            return packer.toByteArray();
        }
    }

    /**
     * {@code POST /tx/{submit,execute}} body: {@code {"ops": [TxOp, ...]}}.
     */
    public static byte[] encodeExecuteBody(List<TxOp> ops) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(1);
            packer.packString("ops"); TxOpCodec.packOps(packer, ops);
            return packer.toByteArray();
        }
    }

    /**
     * {@code POST /db/subscribe} body: {@code {"basis_tx_key": TxKey|nil, "query": str, "args": [QueryArg, ...]}}.
     */
    public static byte[] encodeSubscribeBody(TxKey db, String query, List<QueryArg> args) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packMapHeader(3);
            packer.packString("basis_tx_key");
            if (db == null) packer.packNil();
            else packTxKey(packer, db);
            packer.packString("query"); packer.packString(query);
            packer.packString("args"); QueryArg.packAll(packer, args);
            return packer.toByteArray();
        }
    }

    // ---------------------------------------------------------------
    // Response bodies (server → client)
    // ---------------------------------------------------------------

    public static QueryResult decodeQueryResponse(byte[] body) throws IOException {
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            int fieldCount = unpacker.unpackMapHeader();
            List<ColumnDesc> columns = null;
            List<List<Object>> rows = null;
            for (int i = 0; i < fieldCount; i++) {
                String key = unpacker.unpackString();
                switch (key) {
                    case "columns" -> columns = decodeColumns(unpacker);
                    case "rows" -> rows = decodeRows(unpacker);
                    default -> unpacker.skipValue();
                }
            }
            if (columns == null) throw new IOException("query response missing \"columns\"");
            if (rows == null) throw new IOException("query response missing \"rows\"");
            return new QueryResult(columns, rows);
        }
    }

    private static List<ColumnDesc> decodeColumns(MessageUnpacker unpacker) throws IOException {
        int n = unpacker.unpackArrayHeader();
        var out = new ArrayList<ColumnDesc>(n);
        for (int i = 0; i < n; i++) {
            int fieldCount = unpacker.unpackMapHeader();
            String name = null;
            byte type = 0;
            List<Byte> members = null;
            for (int j = 0; j < fieldCount; j++) {
                String key = unpacker.unpackString();
                switch (key) {
                    case "name" -> name = unpacker.unpackString();
                    case "type" -> type = (byte) unpacker.unpackInt();
                    case "members" -> {
                        int m = unpacker.unpackArrayHeader();
                        members = new ArrayList<>(m);
                        for (int k = 0; k < m; k++) members.add((byte) unpacker.unpackInt());
                    }
                    default -> unpacker.skipValue();
                }
            }
            if (name == null) throw new IOException("column missing \"name\"");
            out.add(new ColumnDesc(name, type, members));
        }
        return out;
    }

    private static List<List<Object>> decodeRows(MessageUnpacker unpacker) throws IOException {
        int n = unpacker.unpackArrayHeader();
        var rows = new ArrayList<List<Object>>(n);
        for (int i = 0; i < n; i++) {
            int rowLen = unpacker.unpackArrayHeader();
            var row = new ArrayList<>(rowLen);
            for (int j = 0; j < rowLen; j++) row.add(DataTypeCodec.unpack(unpacker));
            rows.add(row);
        }
        return rows;
    }

    public static BackendMessage.TxKey decodeTxKey(byte[] body) throws IOException {
        var fields = readFields(body);
        long txId = expectLong(fields, "tx_id");
        Instant systemTime = expectInstant(fields, "system_time");
        return new BackendMessage.TxKey(txId, systemTime);
    }

    public static BackendMessage.TxResult decodeTxResult(byte[] body) throws IOException {
        var fields = readFields(body);
        byte status = toU8(expectLong(fields, "status"), "status");
        long txId = expectLong(fields, "tx_id");
        Instant systemTime = expectInstant(fields, "system_time");
        String err = optionalString(fields, "error_message");
        return new BackendMessage.TxResult(status, txId, systemTime, err);
    }

    public static BackendMessage.ErrorResponse decodeErrorResponse(byte[] body) throws IOException {
        var fields = readFields(body);
        String severityStr = expectString(fields, "severity");
        byte severity = switch (severityStr) {
            case "E" -> MessageTypes.SEVERITY_ERROR;
            case "F" -> MessageTypes.SEVERITY_FATAL;
            default -> throw new IOException("invalid severity: " + severityStr);
        };
        short code = toU16(expectLong(fields, "code"), "code");
        String message = expectString(fields, "message");
        String detail = optionalString(fields, "detail");
        String hint = optionalString(fields, "hint");
        return new BackendMessage.ErrorResponse(severity, code, message, detail, hint);
    }

    // ---------------------------------------------------------------
    // Subscription frame decoding (POST /db/subscribe stream)
    // ---------------------------------------------------------------

    /**
     * Decode one subscription frame (a bare msgpack map) from the stream.
     */
    public static SubscriptionFrame decodeSubscriptionFrame(MessageUnpacker unpacker) throws IOException {
        int n = unpacker.unpackMapHeader();
        String kind = null;
        TxKey basis = null;
        List<ColumnDesc> columns = null;
        List<Row> rows = null;
        String severity = null, message = null, detail = null, hint = null;
        long code = 0;
        for (int i = 0; i < n; i++) {
            String key = unpacker.unpackString();
            switch (key) {
                case "kind" -> kind = unpacker.unpackString();
                case "tx_key" -> basis = unpackTxKey(unpacker, "tx_key");
                case "columns" -> columns = decodeColumns(unpacker);
                case "rows" -> rows = decodeDeltaRows(unpacker);
                case "severity" -> severity = unpacker.unpackString();
                case "code" -> code = unpacker.unpackLong();
                case "message" -> message = unpacker.unpackString();
                case "detail" -> detail = unpackNullableString(unpacker);
                case "hint" -> hint = unpackNullableString(unpacker);
                default -> unpacker.skipValue();
            }
        }
        if (kind == null) throw new IOException("subscription frame missing \"kind\"");
        return switch (kind) {
            case "open" -> {
                if (basis == null) throw new IOException("open frame missing \"tx_key\"");
                yield new SubscriptionFrame.Open(basis, columns == null ? List.of() : columns);
            }
            case "delta" -> {
                if (basis == null) throw new IOException("delta frame missing \"tx_key\"");
                yield new Delta(basis, rows == null ? List.of() : rows);
            }
            case "error" -> {
                byte sev = switch (severity == null ? "" : severity) {
                    case "E" -> MessageTypes.SEVERITY_ERROR;
                    case "F" -> MessageTypes.SEVERITY_FATAL;
                    default -> throw new IOException("invalid severity: " + severity);
                };
                yield new SubscriptionFrame.Error(
                        new BackendMessage.ErrorResponse(sev, toU16(code, "code"), message, detail, hint));
            }
            default -> throw new IOException("unknown subscription frame kind: " + kind);
        };
    }

    private static TxKey unpackTxKey(MessageUnpacker unpacker, String field) throws IOException {
        if (unpacker.tryUnpackNil()) throw new IOException(field + " cannot be nil");
        return unpackTxKeyMap(unpacker);
    }

    private static TxKey unpackTxKeyMap(MessageUnpacker unpacker) throws IOException {
        int n = unpacker.unpackMapHeader();
        long txId = 0;
        Instant systemTime = null;
        for (int i = 0; i < n; i++) {
            String key = unpacker.unpackString();
            switch (key) {
                case "tx_id" -> txId = unpacker.unpackLong();
                case "system_time" -> systemTime = unpacker.unpackTimestamp();
                default -> unpacker.skipValue();
            }
        }
        return new TxKey(txId, systemTime);
    }

    private static String unpackNullableString(MessageUnpacker unpacker) throws IOException {
        if (unpacker.tryUnpackNil()) return null;
        return unpacker.unpackString();
    }

    private static List<Row> decodeDeltaRows(MessageUnpacker unpacker) throws IOException {
        int n = unpacker.unpackArrayHeader();
        var rows = new ArrayList<Row>(n);
        for (int i = 0; i < n; i++) {
            unpacker.unpackArrayHeader(); // [values, weight]
            int valueLen = unpacker.unpackArrayHeader();
            var values = new ArrayList<Object>(valueLen);
            for (int j = 0; j < valueLen; j++) values.add(DataTypeCodec.unpack(unpacker));
            long weight = unpacker.unpackLong();
            rows.add(new Row(values, weight));
        }
        return rows;
    }

    // ---------------------------------------------------------------
    // Field-map helpers
    // ---------------------------------------------------------------

    private static void packTxKey(MessagePacker packer, TxKey basis) throws IOException {
        packer.packMapHeader(2);
        packer.packString("tx_id"); packer.packLong(basis.txId());
        packer.packString("system_time"); packer.packTimestamp(basis.systemTime());
    }

    private static Map<String, Object> readFields(byte[] body) throws IOException {
        try (var unpacker = MessagePack.newDefaultUnpacker(body)) {
            int n = unpacker.unpackMapHeader();
            var map = new LinkedHashMap<String, Object>();
            for (int i = 0; i < n; i++) {
                String key = unpacker.unpackString();
                map.put(key, DataTypeCodec.unpack(unpacker));
            }
            return map;
        }
    }

    private static Object require(Map<String, Object> fields, String name) throws IOException {
        Object v = fields.get(name);
        if (v == null) throw new IOException("missing field: " + name);
        return v;
    }

    private static long expectLong(Map<String, Object> fields, String name) throws IOException {
        Object v = require(fields, name);
        if (v instanceof Long l) return l;
        throw new IOException("field " + name + " expected integer, got " + v.getClass().getName());
    }

    private static String expectString(Map<String, Object> fields, String name) throws IOException {
        Object v = require(fields, name);
        if (v instanceof String s) return s;
        throw new IOException("field " + name + " expected string, got " + v.getClass().getName());
    }

    private static Instant expectInstant(Map<String, Object> fields, String name) throws IOException {
        Object v = require(fields, name);
        if (v instanceof Instant inst) return inst;
        throw new IOException("field " + name + " expected timestamp, got " + v.getClass().getName());
    }

    private static String optionalString(Map<String, Object> fields, String name) throws IOException {
        Object v = fields.get(name);
        if (v == null) return null;
        if (v instanceof String s) return s;
        throw new IOException("field " + name + " expected string or nil, got " + v.getClass().getName());
    }

    private static int toInt(long v) {
        if (v < 0 || v > 0xFFFFFFFFL) throw new IllegalArgumentException("u32 out of range: " + v);
        return (int) v;
    }

    private static byte toU8(long v, String field) throws IOException {
        if (v < 0 || v > 0xFFL) throw new IOException(field + " out of u8 range: " + v);
        return (byte) v;
    }

    private static short toU16(long v, String field) throws IOException {
        if (v < 0 || v > 0xFFFFL) throw new IOException(field + " out of u16 range: " + v);
        return (short) v;
    }
}
