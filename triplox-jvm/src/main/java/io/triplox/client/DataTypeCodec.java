package io.triplox.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import clojure.lang.Keyword;

import static io.triplox.client.MessageTypes.*;

/**
 * Encodes and decodes DataType values on the wire.
 *
 * Type mappings:
 *   BigInt (i128)  → BigInteger
 *   Boolean        → Boolean
 *   Bytes          → byte[]
 *   Double         → Double
 *   Float          → Float
 *   Instant        → java.time.Instant
 *   Long           → Long
 *   Ref            → UnsupportedOperationException
 *   String         → String
 *   Tuple          → List<Object>
 *   Uuid           → UUID
 *   Vector         → List<Object>
 *   Map            → TreeMap<String, Object>
 *   Keyword        → clojure.lang.Keyword
 */
public final class DataTypeCodec {
    private DataTypeCodec() {}

    // ---------------------------------------------------------------
    // Encoding
    // ---------------------------------------------------------------

    public static void encode(DataOutputStream out, Object value) throws IOException {
        switch (value) {
            case BigInteger bi -> {
                out.writeByte(TAG_BIG_INT);
                encodeI128(out, bi);
            }
            case Boolean b -> {
                out.writeByte(TAG_BOOLEAN);
                out.writeBoolean(b);
            }
            case byte[] bytes -> {
                out.writeByte(TAG_BYTES);
                encodeBytes(out, bytes);
            }
            case Double d -> {
                out.writeByte(TAG_DOUBLE);
                out.writeDouble(d);
            }
            case Float f -> {
                out.writeByte(TAG_FLOAT);
                out.writeFloat(f);
            }
            case Instant inst -> {
                out.writeByte(TAG_INSTANT);
                long micros = inst.getEpochSecond() * 1_000_000 + inst.getNano() / 1000;
                out.writeLong(micros);
            }
            case Long l -> {
                out.writeByte(TAG_LONG);
                out.writeLong(l);
            }
            case String s -> {
                out.writeByte(TAG_STRING);
                encodeString(out, s);
            }
            case UUID uuid -> {
                out.writeByte(TAG_UUID);
                out.writeLong(uuid.getMostSignificantBits());
                out.writeLong(uuid.getLeastSignificantBits());
            }
            case Keyword kw -> {
                out.writeByte(TAG_KEYWORD);
                encodeString(out, kw.toString());
            }
            case TreeMap<?, ?> map -> {
                out.writeByte(TAG_MAP);
                @SuppressWarnings("unchecked")
                var typedMap = (TreeMap<String, Object>) map;
                encodeDataTypeMap(out, typedMap);
            }
            case List<?> list -> {
                // Default lists to Vector encoding
                out.writeByte(TAG_VECTOR);
                encodeDataTypeVec(out, list);
            }
            case TaggedTuple tt -> {
                out.writeByte(TAG_TUPLE);
                encodeDataTypeVec(out, tt.elements());
            }
            default -> throw new IllegalArgumentException("Cannot encode value of type: " + value.getClass().getName());
        }
    }

    // ---------------------------------------------------------------
    // Decoding
    // ---------------------------------------------------------------

    public static Object decode(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return decodeByTag(in, tag);
    }

    static Object decodeByTag(DataInputStream in, byte tag) throws IOException {
        return switch (tag) {
            case TAG_BIG_INT -> decodeI128(in);
            case TAG_BOOLEAN -> in.readBoolean();
            case TAG_BYTES -> decodeByteArray(in);
            case TAG_DOUBLE -> in.readDouble();
            case TAG_FLOAT -> in.readFloat();
            case TAG_INSTANT -> {
                long micros = in.readLong();
                long secs = Math.floorDiv(micros, 1_000_000);
                long microRem = Math.floorMod(micros, 1_000_000);
                yield Instant.ofEpochSecond(secs, microRem * 1000);
            }
            case TAG_LONG -> in.readLong();
            case TAG_REF -> throw new UnsupportedOperationException("Ref type is not yet supported");
            case TAG_STRING -> decodeString(in);
            case TAG_TUPLE -> {
                int count = in.readInt();
                var list = new ArrayList<>(count);
                for (int i = 0; i < count; i++) list.add(decode(in));
                yield new TaggedTuple(list);
            }
            case TAG_UUID -> new UUID(in.readLong(), in.readLong());
            case TAG_VECTOR -> decodeDataTypeVec(in);
            case TAG_MAP -> decodeDataTypeMap(in);
            case TAG_KEYWORD -> {
                String s = decodeString(in);
                yield parseKeyword(s);
            }
            default -> throw new IOException("Unknown DataType tag: " + (tag & 0xFF));
        };
    }

    // ---------------------------------------------------------------
    // Helpers: String
    // ---------------------------------------------------------------

    public static void encodeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static String decodeString(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] bytes = new byte[len];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    // ---------------------------------------------------------------
    // Helpers: Bytes
    // ---------------------------------------------------------------

    static void encodeBytes(DataOutputStream out, byte[] b) throws IOException {
        out.writeInt(b.length);
        out.write(b);
    }

    static byte[] decodeByteArray(DataInputStream in) throws IOException {
        int len = in.readInt();
        byte[] b = new byte[len];
        in.readFully(b);
        return b;
    }

    // ---------------------------------------------------------------
    // Helpers: i128 → BigInteger
    // ---------------------------------------------------------------

    static void encodeI128(DataOutputStream out, BigInteger bi) throws IOException {
        byte[] raw = bi.toByteArray();
        // Pad or trim to exactly 16 bytes (big-endian, sign-extended)
        byte[] buf = new byte[16];
        byte fill = (bi.signum() < 0) ? (byte) 0xFF : (byte) 0x00;
        java.util.Arrays.fill(buf, fill);
        int srcStart = Math.max(0, raw.length - 16);
        int dstStart = Math.max(0, 16 - raw.length);
        int copyLen = Math.min(raw.length, 16);
        System.arraycopy(raw, srcStart, buf, dstStart, copyLen);
        out.write(buf);
    }

    static BigInteger decodeI128(DataInputStream in) throws IOException {
        byte[] buf = new byte[16];
        in.readFully(buf);
        return new BigInteger(buf);
    }

    // ---------------------------------------------------------------
    // Helpers: Optional
    // ---------------------------------------------------------------

    public static void encodeOptionalLong(DataOutputStream out, Long value) throws IOException {
        if (value == null) {
            out.writeByte(0x00);
        } else {
            out.writeByte(0x01);
            out.writeLong(value);
        }
    }

    public static Long decodeOptionalLong(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case 0x00 -> null;
            case 0x01 -> in.readLong();
            default -> throw new IOException("Invalid option tag: 0x" + Integer.toHexString(tag & 0xFF));
        };
    }

    public static void encodeOptionalString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeByte(0x00);
        } else {
            out.writeByte(0x01);
            encodeString(out, value);
        }
    }

    public static String decodeOptionalString(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case 0x00 -> null;
            case 0x01 -> decodeString(in);
            default -> throw new IOException("Invalid option tag: 0x" + Integer.toHexString(tag & 0xFF));
        };
    }

    // ---------------------------------------------------------------
    // Helpers: Vec<DataType>, Map<String, DataType>
    // ---------------------------------------------------------------

    static void encodeDataTypeVec(DataOutputStream out, List<?> list) throws IOException {
        out.writeInt(list.size());
        for (Object item : list) encode(out, item);
    }

    static List<Object> decodeDataTypeVec(DataInputStream in) throws IOException {
        int count = in.readInt();
        var list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) list.add(decode(in));
        return list;
    }

    public static void encodeDataTypeMap(DataOutputStream out, TreeMap<String, Object> map) throws IOException {
        out.writeInt(map.size());
        for (var entry : map.entrySet()) {
            encodeString(out, entry.getKey());
            encode(out, entry.getValue());
        }
    }

    static TreeMap<String, Object> decodeDataTypeMap(DataInputStream in) throws IOException {
        int count = in.readInt();
        var map = new TreeMap<String, Object>();
        for (int i = 0; i < count; i++) {
            String key = decodeString(in);
            Object value = decode(in);
            map.put(key, value);
        }
        return map;
    }

    // ---------------------------------------------------------------
    // Helpers: String Map (Map<String, String>)
    // ---------------------------------------------------------------

    public static void encodeStringMap(DataOutputStream out, TreeMap<String, String> map) throws IOException {
        out.writeInt(map.size());
        for (var entry : map.entrySet()) {
            encodeString(out, entry.getKey());
            encodeString(out, entry.getValue());
        }
    }

    public static TreeMap<String, String> decodeStringMap(DataInputStream in) throws IOException {
        int count = in.readInt();
        var map = new TreeMap<String, String>();
        for (int i = 0; i < count; i++) {
            String key = decodeString(in);
            String value = decodeString(in);
            map.put(key, value);
        }
        return map;
    }

    // ---------------------------------------------------------------
    // Keyword parsing
    // ---------------------------------------------------------------

    static Keyword parseKeyword(String s) {
        // Wire format is ":ns/name" or ":name"
        String stripped = s.startsWith(":") ? s.substring(1) : s;
        int slash = stripped.indexOf('/');
        if (slash >= 0) {
            return Keyword.intern(stripped.substring(0, slash), stripped.substring(slash + 1));
        } else {
            return Keyword.intern(stripped);
        }
    }

    /**
     * Wrapper to distinguish Tuple from Vector during encoding.
     * Decoded tuples come back as TaggedTuple; vectors as List.
     */
    public record TaggedTuple(List<Object> elements) {}
}
