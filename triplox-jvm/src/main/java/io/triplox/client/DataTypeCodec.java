package io.triplox.client;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import clojure.lang.Keyword;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.core.MessageFormat;
import org.msgpack.value.ValueType;

import static io.triplox.client.MessageTypes.*;

/**
 * MessagePack codec for {@code DataType} values on the HTTP/2 wire.
 *
 * <p>Type mappings (must match {@code crate::msgpack_codec}):</p>
 * <ul>
 *   <li>BigInteger → ext 1 (16 bytes, big-endian i128)</li>
 *   <li>Boolean    → bool</li>
 *   <li>byte[]     → bin</li>
 *   <li>Double     → float64</li>
 *   <li>Float      → float32</li>
 *   <li>Instant    → ext -1 (msgpack Timestamp, nanosecond)</li>
 *   <li>Long       → int</li>
 *   <li>String     → str</li>
 *   <li>UUID       → ext 2 (16 bytes)</li>
 *   <li>Keyword    → ext 3 (UTF-8 "ns/name", no leading colon)</li>
 *   <li>List       → array</li>
 *   <li>Map        → map (string keys)</li>
 * </ul>
 *
 * <p>The encoder iterates the source map as given — no key sorting is imposed.
 * Tests that need byte-deterministic output should construct deterministic-order
 * maps themselves (e.g. {@link java.util.TreeMap} or pre-sorted
 * {@link LinkedHashMap}).</p>
 */
public final class DataTypeCodec {
    private DataTypeCodec() {}

    private static final BigInteger I128_MIN = BigInteger.ONE.shiftLeft(127).negate();
    private static final BigInteger I128_MAX = BigInteger.ONE.shiftLeft(127).subtract(BigInteger.ONE);

    // ---------------------------------------------------------------
    // Pack
    // ---------------------------------------------------------------

    public static void pack(MessagePacker packer, Object value) throws IOException {
        switch (value) {
            case Boolean b -> packer.packBoolean(b);
            case Long l -> packer.packLong(l);
            case Float f -> packer.packFloat(f);
            case Double d -> packer.packDouble(d);
            case String s -> packer.packString(s);
            case byte[] bytes -> {
                packer.packBinaryHeader(bytes.length);
                packer.writePayload(bytes);
            }
            case BigInteger bi -> packBigInteger(packer, bi);
            case UUID uuid -> packUuid(packer, uuid);
            case Keyword kw -> packKeyword(packer, kw);
            case Instant inst -> packer.packTimestamp(inst);
            case Map<?, ?> map -> packMap(packer, map);
            case List<?> list -> packList(packer, list);
            default -> throw new IllegalArgumentException(
                    "Cannot encode value of type: " + value.getClass().getName());
        }
    }

    private static void packList(MessagePacker packer, List<?> list) throws IOException {
        packer.packArrayHeader(list.size());
        for (Object item : list) {
            pack(packer, item);
        }
    }

    private static void packMap(MessagePacker packer, Map<?, ?> map) throws IOException {
        packer.packMapHeader(map.size());
        for (var entry : map.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IllegalArgumentException(
                        "Map keys must be String, got: " + entry.getKey().getClass().getName());
            }
            packer.packString(key);
            pack(packer, entry.getValue());
        }
    }

    public static void packKeyword(MessagePacker packer, Keyword kw) throws IOException {
        byte[] payload = keywordToWire(kw).getBytes(StandardCharsets.UTF_8);
        packer.packExtensionTypeHeader(EXT_KEYWORD, payload.length);
        packer.writePayload(payload);
    }

    public static void packBigInteger(MessagePacker packer, BigInteger bi) throws IOException {
        packer.packExtensionTypeHeader(EXT_BIGINT, 16);
        packer.writePayload(toBigEndianI128(bi));
    }

    public static void packUuid(MessagePacker packer, UUID uuid) throws IOException {
        var buf = ByteBuffer.allocate(16);
        buf.putLong(uuid.getMostSignificantBits());
        buf.putLong(uuid.getLeastSignificantBits());
        packer.packExtensionTypeHeader(EXT_UUID, 16);
        packer.writePayload(buf.array());
    }

    // ---------------------------------------------------------------
    // Unpack
    // ---------------------------------------------------------------

    public static Object unpack(MessageUnpacker unpacker) throws IOException {
        ValueType type = unpacker.getNextFormat().getValueType();
        return switch (type) {
            case NIL -> {
                unpacker.unpackNil();
                yield null;
            }
            case BOOLEAN -> unpacker.unpackBoolean();
            case INTEGER -> unpacker.unpackLong();
            case FLOAT -> {
                if (unpacker.getNextFormat() == MessageFormat.FLOAT32) {
                    yield Float.valueOf(unpacker.unpackFloat());
                }
                yield Double.valueOf(unpacker.unpackDouble());
            }
            case STRING -> unpacker.unpackString();
            case BINARY -> {
                int len = unpacker.unpackBinaryHeader();
                yield unpacker.readPayload(len);
            }
            case ARRAY -> {
                int count = unpacker.unpackArrayHeader();
                var list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) {
                    list.add(unpack(unpacker));
                }
                yield list;
            }
            case MAP -> {
                int count = unpacker.unpackMapHeader();
                var map = new LinkedHashMap<String, Object>();
                for (int i = 0; i < count; i++) {
                    String key = unpacker.unpackString();
                    map.put(key, unpack(unpacker));
                }
                yield map;
            }
            case EXTENSION -> {
                var header = unpacker.unpackExtensionTypeHeader();
                yield unpackExtension(unpacker, header.getType(), header.getLength());
            }
        };
    }

    private static Object unpackExtension(MessageUnpacker unpacker, byte type, int length)
            throws IOException {
        return switch (type) {
            case EXT_TIMESTAMP -> {
                // msgpack-core has a built-in helper, but we already consumed the header.
                // Read the payload bytes and decode manually.
                byte[] payload = unpacker.readPayload(length);
                yield decodeTimestamp(payload);
            }
            case EXT_BIGINT -> {
                if (length != 16) {
                    throw new IOException("BigInt ext payload must be 16 bytes, got " + length);
                }
                byte[] payload = unpacker.readPayload(16);
                yield new BigInteger(payload);
            }
            case EXT_UUID -> {
                if (length != 16) {
                    throw new IOException("Uuid ext payload must be 16 bytes, got " + length);
                }
                byte[] payload = unpacker.readPayload(16);
                var buf = ByteBuffer.wrap(payload);
                yield new UUID(buf.getLong(), buf.getLong());
            }
            case EXT_KEYWORD -> {
                byte[] payload = unpacker.readPayload(length);
                yield keywordFromWire(new String(payload, StandardCharsets.UTF_8));
            }
            default -> throw new IOException("Unknown msgpack ext type: " + type);
        };
    }

    // ---------------------------------------------------------------
    // Keyword wire format (no leading `:`)
    // ---------------------------------------------------------------

    static String keywordToWire(Keyword kw) {
        String ns = kw.getNamespace();
        return ns == null ? kw.getName() : ns + "/" + kw.getName();
    }

    static Keyword keywordFromWire(String s) {
        if (s.isEmpty()) {
            throw new IllegalArgumentException("empty keyword");
        }
        int slash = s.indexOf('/');
        if (slash < 0) {
            return Keyword.intern(s);
        }
        if (slash == 0 || slash == s.length() - 1) {
            throw new IllegalArgumentException("invalid keyword: " + s);
        }
        return Keyword.intern(s.substring(0, slash), s.substring(slash + 1));
    }

    /**
     * Parse a Datalog-style keyword string ({@code ":ns/name"} or {@code ":name"})
     * into a {@link Keyword}. Kept for the Clojure layer.
     */
    public static Keyword parseKeyword(String s) {
        String stripped = s.startsWith(":") ? s.substring(1) : s;
        return keywordFromWire(stripped);
    }

    // ---------------------------------------------------------------
    // i128 BigInteger helpers
    // ---------------------------------------------------------------

    static byte[] toBigEndianI128(BigInteger bi) throws IOException {
        if (bi.compareTo(I128_MIN) < 0 || bi.compareTo(I128_MAX) > 0) {
            throw new IOException("BigInteger out of i128 range: " + bi);
        }
        byte[] raw = bi.toByteArray();
        byte[] buf = new byte[16];
        byte fill = (bi.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        java.util.Arrays.fill(buf, fill);
        int srcStart = Math.max(0, raw.length - 16);
        int dstStart = Math.max(0, 16 - raw.length);
        int copyLen = Math.min(raw.length, 16);
        System.arraycopy(raw, srcStart, buf, dstStart, copyLen);
        return buf;
    }

    // ---------------------------------------------------------------
    // Timestamp ext (-1) decode
    // ---------------------------------------------------------------

    private static Instant decodeTimestamp(byte[] payload) throws IOException {
        long secs;
        long nanos;
        switch (payload.length) {
            case 4 -> {
                secs = ByteBuffer.wrap(payload).getInt() & 0xFFFFFFFFL;
                nanos = 0;
            }
            case 8 -> {
                long data = ByteBuffer.wrap(payload).getLong();
                nanos = (data >>> 34) & 0x3FFFFFFFL;
                secs = data & 0x3FFFFFFFFL;
            }
            case 12 -> {
                ByteBuffer buf = ByteBuffer.wrap(payload);
                nanos = buf.getInt() & 0xFFFFFFFFL;
                secs = buf.getLong();
            }
            default -> throw new IOException("Invalid Timestamp ext payload length: " + payload.length);
        }
        return Instant.ofEpochSecond(secs, nanos);
    }
}
