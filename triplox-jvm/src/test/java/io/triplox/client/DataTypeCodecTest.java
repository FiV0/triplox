package io.triplox.client;

import org.junit.jupiter.api.Test;

import java.io.*;
import java.math.BigInteger;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.UUID;

import clojure.lang.Keyword;

import static org.junit.jupiter.api.Assertions.*;

class DataTypeCodecTest {

    private Object roundtrip(Object value) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        DataTypeCodec.encode(dos, value);
        dos.flush();

        var bin = new ByteArrayInputStream(baos.toByteArray());
        var dis = new DataInputStream(bin);
        return DataTypeCodec.decode(dis);
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
    void testBooleanTrue() throws IOException {
        assertEquals(true, roundtrip(true));
    }

    @Test
    void testBooleanFalse() throws IOException {
        assertEquals(false, roundtrip(false));
    }

    @Test
    void testBytes() throws IOException {
        byte[] input = new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF};
        assertArrayEquals(input, (byte[]) roundtrip(input));
    }

    @Test
    void testBytesEmpty() throws IOException {
        byte[] input = new byte[0];
        assertArrayEquals(input, (byte[]) roundtrip(input));
    }

    @Test
    void testDouble() throws IOException {
        assertEquals(Math.PI, roundtrip(Math.PI));
    }

    @Test
    void testFloat() throws IOException {
        assertEquals((float) Math.E, roundtrip((float) Math.E));
    }

    @Test
    void testInstant() throws IOException {
        // Microsecond precision only
        Instant original = Instant.ofEpochSecond(1700000000L, 123456000);
        Object result = roundtrip(original);
        assertEquals(original, result);
    }

    @Test
    void testInstantPreEpoch() throws IOException {
        Instant original = Instant.ofEpochSecond(-100, 500000000);
        // Pre-epoch timestamps lose sub-microsecond precision
        long micros = original.getEpochSecond() * 1_000_000 + original.getNano() / 1000;
        Instant expected = Instant.ofEpochSecond(
                Math.floorDiv(micros, 1_000_000),
                Math.floorMod(micros, 1_000_000) * 1000);
        assertEquals(expected, roundtrip(original));
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
    }

    @Test
    void testStringEmpty() throws IOException {
        assertEquals("", roundtrip(""));
    }

    @Test
    void testStringUnicode() throws IOException {
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
    }

    @Test
    void testVectorEmpty() throws IOException {
        var list = new ArrayList<>();
        var result = (List<?>) roundtrip(list);
        assertEquals(0, result.size());
    }

    @Test
    void testMap() throws IOException {
        var map = new TreeMap<String, Object>();
        map.put("age", 30L);
        map.put("name", "alice");
        @SuppressWarnings("unchecked")
        var result = (TreeMap<String, Object>) roundtrip(map);
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
        var innerResult = (TreeMap<String, Object>) result.get(0);
        assertEquals(1L, innerResult.get("x"));
        assertEquals("hello", result.get(1));
    }
}
