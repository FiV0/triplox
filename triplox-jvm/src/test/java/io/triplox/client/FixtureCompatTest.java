package io.triplox.client;

import clojure.lang.Keyword;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

import static io.triplox.client.MessageTypes.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Cross-language byte fixtures: the JVM client must produce and consume
 * the exact same msgpack bytes as the Rust codec for canonical inputs.
 *
 * <p>Fixtures live at {@code tests/fixtures/} at the repo root. They are
 * generated and pinned by the Rust integration test
 * {@code tests/fixture_compat_test.rs}. To regenerate after an intentional
 * encoding change, set {@code TRIPLOX_REGEN_FIXTURES=1} on the Rust side
 * and rerun.</p>
 */
class FixtureCompatTest {

    private static final Path FIXTURES_DIR = Path.of("..", "tests", "fixtures");

    private static byte[] readFixture(String name) throws IOException {
        return Files.readAllBytes(FIXTURES_DIR.resolve(name));
    }

    /** Mirrors the canonical input in {@code tests/fixture_compat_test.rs::data_type_zoo}. */
    private static Map<String, Object> dataTypeZoo() {
        var inner = new TreeMap<String, Object>();
        inner.put("nested_long", 1L);

        var m = new TreeMap<String, Object>();
        m.put("bigint", new BigInteger("170141183460469231731687303715884105727")); // i128::MAX
        m.put("boolean", true);
        m.put("bytes", new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});
        m.put("double", Math.PI);
        m.put("float", 1.5f);
        m.put("instant", Instant.ofEpochSecond(1_700_000_000L, 123_456_789));
        m.put("keyword", Keyword.intern("person", "name"));
        m.put("long", 42L);
        m.put("map", inner);
        m.put("string", "hello 世界");
        m.put("uuid", UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        m.put("vector", List.of(1L, 2L, 3L));
        return m;
    }

    @Test
    void dataTypeZooEncodesIdentically() throws IOException {
        byte[] expected = readFixture("data_type_zoo.mpk");
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            DataTypeCodec.pack(packer, dataTypeZoo());
            byte[] actual = packer.toByteArray();
            assertArrayEquals(expected, actual,
                    "JVM-encoded bytes must match the Rust-produced fixture");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void dataTypeZooDecodesToExpectedValues() throws IOException {
        byte[] bytes = readFixture("data_type_zoo.mpk");
        try (var unpacker = MessagePack.newDefaultUnpacker(bytes)) {
            var decoded = (Map<String, Object>) DataTypeCodec.unpack(unpacker);

            assertEquals(new BigInteger("170141183460469231731687303715884105727"),
                    decoded.get("bigint"));
            assertEquals(true, decoded.get("boolean"));
            assertArrayEquals(new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF},
                    (byte[]) decoded.get("bytes"));
            assertEquals(Math.PI, (Double) decoded.get("double"));
            // float32 round-trips as Double via msgpack-core's unpack
            assertEquals(1.5, ((Double) decoded.get("float")).doubleValue(), 1e-9);
            assertEquals(Instant.ofEpochSecond(1_700_000_000L, 123_456_789),
                    decoded.get("instant"));
            assertEquals(Keyword.intern("person", "name"), decoded.get("keyword"));
            assertEquals(42L, decoded.get("long"));
            assertEquals("hello 世界", decoded.get("string"));
            assertEquals(UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                    decoded.get("uuid"));
            assertEquals(List.of(1L, 2L, 3L), decoded.get("vector"));

            var nested = (Map<String, Object>) decoded.get("map");
            assertEquals(1L, nested.get("nested_long"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void queryResponseFixtureRoundTrips() throws IOException {
        byte[] bytes = readFixture("query_response.mpk");
        var result = WireCodec.decodeQueryResponse(bytes);
        assertEquals(2, result.columns().size());
        assertEquals("?e", result.columns().get(0).name());
        assertEquals(TAG_LONG, result.columns().get(0).dataType());
        assertEquals("?name", result.columns().get(1).name());
        assertEquals(TAG_STRING, result.columns().get(1).dataType());
        assertEquals(2, result.rows().size());
        assertEquals(1L, result.rows().get(0).get(0));
        assertEquals("alice", result.rows().get(0).get(1));
        assertEquals(2L, result.rows().get(1).get(0));
        assertEquals("bob", result.rows().get(1).get(1));
    }

    @Test
    void errorResponseFixtureRoundTrips() throws IOException {
        byte[] bytes = readFixture("error_response.mpk");
        var err = WireCodec.decodeErrorResponse(bytes);
        assertEquals(SEVERITY_ERROR, err.severity());
        assertEquals((short) 2001, err.code());
        assertEquals("parse error: unexpected token", err.message());
        assertEquals("near position 17", err.detail());
        assertNull(err.hint());
    }
}
