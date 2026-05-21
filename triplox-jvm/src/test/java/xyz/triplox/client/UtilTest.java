package xyz.triplox.client;

import clojure.lang.Keyword;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class UtilTest {
    @Test
    void testListPreservesItems() {
        assertEquals(List.of("a", "b", "c"), Util.list("a", "b", "c"));
    }

    @Test
    void testKwParsesColonPrefixedKeyword() {
        assertEquals(Keyword.intern("db.type", "string"), Util.kw(":db.type/string"));
        assertEquals(Keyword.intern("name"), Util.kw(":name"));
    }

    @Test
    void testMapPreservesInsertionOrder() {
        var map = Util.map(":db/id", 1L, ":name", "alice");
        assertEquals(List.of(":db/id", ":name"), List.copyOf(map.keySet()));
        assertEquals(1L, map.get(":db/id"));
        assertEquals("alice", map.get(":name"));
    }

    @Test
    void testMapRejectsOddArgumentCount() {
        assertThrows(IllegalArgumentException.class, () -> Util.map(":db/id", 1L, ":name"));
    }

    @Test
    void testMapRejectsNonStringKeys() {
        assertThrows(IllegalArgumentException.class, () -> Util.map(1L, "alice"));
    }
}
