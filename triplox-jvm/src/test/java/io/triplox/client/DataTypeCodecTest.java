package io.triplox.client;

import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import clojure.lang.Keyword;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeCodecTest {

    private Object roundtrip(Object value) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            DataTypeCodec.pack(packer, value);
            byte[] bytes = packer.toByteArray();
            try (var unpacker = MessagePack.newDefaultUnpacker(bytes)) {
                Object result = DataTypeCodec.unpack(unpacker);
                assertFalse(unpacker.hasNext(), "trailing bytes after unpack");
                return result;
            }
        }
    }

    @Test
    void testBigIntPositive() throws IOException {
        var bi = new BigInteger("123456789012345678901234567890");
        assertEquals(bi, roundtrip(bi));
    }

    @Test
    void testBigIntNegative() throws IOException {
        var bi = new BigInteger("-123456789012345678901234567890");
        assertEquals(bi, roundtrip(bi));
    }

    @Test
    void testBigIntZero() throws IOException {
        assertEquals(BigInteger.ZERO, roundtrip(BigInteger.ZERO));
    }

    @Test
    void testBoolean() throws IOException {
        assertEquals(true, roundtrip(true));
        assertEquals(false, roundtrip(false));
    }

    @Test
    void testBytes() throws IOException {
        byte[] input = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        assertArrayEquals(input, (byte[]) roundtrip(input));
        assertArrayEquals(new byte[0], (byte[]) roundtrip(new byte[0]));
    }

    @Test
    void testDouble() throws IOException {
        assertEquals(Math.PI, roundtrip(Math.PI));
    }

    @Test
    void testFloat() throws IOException {
        // unpack always returns Double; check via cast
        Object r = roundtrip((float) Math.E);
        assertTrue(r instanceof Double, "msgpack-core unpack returns Double for both float32 and float64");
        assertEquals((double) (float) Math.E, ((Double) r).doubleValue(), 1e-6);
    }

    @Test
    void testInstantPostEpoch() throws IOException {
        Instant original = Instant.ofEpochSecond(1_700_000_000L, 123_456_789);
        assertEquals(original, roundtrip(original));
    }

    @Test
    void testInstantPreEpoch() throws IOException {
        Instant original = Instant.ofEpochSecond(-1_000_000_000L, 500_000_000);
        assertEquals(original, roundtrip(original));
    }

    @Test
    void testLong() throws IOException {
        assertEquals(Long.MAX_VALUE, roundtrip(Long.MAX_VALUE));
        assertEquals(Long.MIN_VALUE, roundtrip(Long.MIN_VALUE));
        assertEquals(0L, roundtrip(0L));
    }

    @Test
    void testString() throws IOException {
        assertEquals("hello world", roundtrip("hello world"));
        assertEquals("", roundtrip(""));
        assertEquals("hello 世界 🌍", roundtrip("hello 世界 🌍"));
    }

    @Test
    void testUuid() throws IOException {
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, roundtrip(uuid));
    }

    @Test
    void testVector() throws IOException {
        var list = new ArrayList<>(List.of(1L, "two", true));
        var result = (List<?>) roundtrip(list);
        assertEquals(3, result.size());
        assertEquals(1L, result.get(0));
        assertEquals("two", result.get(1));
        assertEquals(true, result.get(2));

        // Empty
        assertEquals(0, ((List<?>) roundtrip(new ArrayList<>())).size());
    }

    @Test
    void testMap() throws IOException {
        var map = new TreeMap<String, Object>();
        map.put("age", 30L);
        map.put("name", "alice");
        @SuppressWarnings("unchecked")
        var result = (Map<String, Object>) roundtrip(map);
        assertEquals(30L, result.get("age"));
        assertEquals("alice", result.get("name"));
    }

    @Test
    void testKeywordPlain() throws IOException {
        Keyword kw = Keyword.intern("foo");
        assertEquals(kw, roundtrip(kw));
    }

    @Test
    void testKeywordNamespaced() throws IOException {
        Keyword kw = Keyword.intern("person", "name");
        assertEquals(kw, roundtrip(kw));
    }

    @Test
    void testNested() throws IOException {
        var inner = new TreeMap<String, Object>();
        inner.put("x", 1L);
        var list = new ArrayList<>(List.of(inner, "hello"));
        var result = (List<?>) roundtrip(list);
        assertEquals(2, result.size());
        @SuppressWarnings("unchecked")
        var innerResult = (Map<String, Object>) result.get(0);
        assertEquals(1L, innerResult.get("x"));
        assertEquals("hello", result.get(1));
    }

    @Test
    void testKeywordWireFormatStripsLeadingColon() throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            DataTypeCodec.pack(packer, Keyword.intern("person", "name"));
            byte[] bytes = packer.toByteArray();
            // ext8 marker + len(11) + type(3) + 11 bytes of "person/name"
            assertEquals((byte) 0xc7, bytes[0]);
            assertEquals(11, bytes[1]);
            assertEquals(MessageTypes.EXT_KEYWORD, bytes[2]);
            assertEquals("person/name", new String(bytes, 3, 11));
        }
    }
}
