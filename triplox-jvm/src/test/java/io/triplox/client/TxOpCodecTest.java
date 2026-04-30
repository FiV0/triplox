package io.triplox.client;

import clojure.lang.Keyword;
import org.junit.jupiter.api.Test;
import org.msgpack.core.MessagePack;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class TxOpCodecTest {

    private List<TxOp> roundtrip(List<TxOp> ops) throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            TxOpCodec.packOps(packer, ops);
            byte[] bytes = packer.toByteArray();
            try (var unpacker = MessagePack.newDefaultUnpacker(bytes)) {
                return TxOpCodec.unpackOps(unpacker);
            }
        }
    }

    @Test
    void testPut() throws IOException {
        var doc = new TreeMap<Keyword, Object>();
        doc.put(Keyword.intern("db", "id"), 1L);
        doc.put(Keyword.intern("name"), "alice");
        var result = roundtrip(List.of(new TxOp.Put(doc)));
        assertEquals(1, result.size());
        var put = (TxOp.Put) result.get(0);
        assertEquals(1L, put.document().get(Keyword.intern("db", "id")));
        assertEquals("alice", put.document().get(Keyword.intern("name")));
    }

    @Test
    void testAdd() throws IOException {
        var result = roundtrip(List.of(new TxOp.Add(
                new EntityRef.Id(42),
                Keyword.intern("email"),
                "test@example.com")));
        assertEquals(1, result.size());
        var add = (TxOp.Add) result.get(0);
        assertEquals(42, ((EntityRef.Id) add.entity()).id());
        assertEquals(Keyword.intern("email"), add.attribute());
        assertEquals("test@example.com", add.value());
    }

    @Test
    void testRetract() throws IOException {
        var result = roundtrip(List.of(new TxOp.Retract(
                new EntityRef.Id(42),
                Keyword.intern("email"),
                "old@example.com")));
        assertEquals(1, result.size());
        var ret = (TxOp.Retract) result.get(0);
        assertEquals(42, ((EntityRef.Id) ret.entity()).id());
        assertEquals(Keyword.intern("email"), ret.attribute());
        assertEquals("old@example.com", ret.value());
    }

    @Test
    void testDelete() throws IOException {
        var result = roundtrip(List.of(new TxOp.Delete(new EntityRef.Id(99))));
        assertEquals(1, result.size());
        assertEquals(new TxOp.Delete(new EntityRef.Id(99)), result.get(0));
    }

    @Test
    void testErase() throws IOException {
        var result = roundtrip(List.of(new TxOp.Erase(new EntityRef.Id(100))));
        assertEquals(1, result.size());
        assertEquals(new TxOp.Erase(new EntityRef.Id(100)), result.get(0));
    }

    @Test
    void testMultipleOps() throws IOException {
        var doc = new TreeMap<Keyword, Object>();
        doc.put(Keyword.intern("db", "id"), 1L);
        doc.put(Keyword.intern("name"), "bob");

        var ops = List.<TxOp>of(
                new TxOp.Put(doc),
                new TxOp.Add(new EntityRef.Id(1), Keyword.intern("age"), 30L),
                new TxOp.Retract(new EntityRef.Id(1), Keyword.intern("name"), "old-bob"),
                new TxOp.Delete(new EntityRef.Id(99)),
                new TxOp.Erase(new EntityRef.Id(100))
        );
        var result = roundtrip(ops);
        assertEquals(5, result.size());
        assertInstanceOf(TxOp.Put.class, result.get(0));
        assertInstanceOf(TxOp.Add.class, result.get(1));
        assertInstanceOf(TxOp.Retract.class, result.get(2));
        assertInstanceOf(TxOp.Delete.class, result.get(3));
        assertInstanceOf(TxOp.Erase.class, result.get(4));
    }

    @Test
    void testEntityRefVariants() throws IOException {
        var ops = List.<TxOp>of(
                new TxOp.Delete(new EntityRef.Id(42)),
                new TxOp.Delete(new EntityRef.TempId("temp-1")),
                new TxOp.Delete(new EntityRef.Ident(Keyword.intern("db", "ident"))),
                new TxOp.Delete(new EntityRef.LookupRef(Keyword.intern("email"), "test@example.com"))
        );
        var result = roundtrip(ops);
        assertEquals(4, result.size());
        assertEquals(new EntityRef.Id(42), ((TxOp.Delete) result.get(0)).entity());
        assertEquals(new EntityRef.TempId("temp-1"), ((TxOp.Delete) result.get(1)).entity());
        assertEquals(new EntityRef.Ident(Keyword.intern("db", "ident")), ((TxOp.Delete) result.get(2)).entity());
        assertEquals(new EntityRef.LookupRef(Keyword.intern("email"), "test@example.com"), ((TxOp.Delete) result.get(3)).entity());
    }

    @Test
    void testRefValueAsLong() throws IOException {
        var result = roundtrip(List.of(new TxOp.Add(
                new EntityRef.Id(1),
                Keyword.intern("friend"),
                2L)));
        assertEquals(1, result.size());
        var add = (TxOp.Add) result.get(0);
        assertEquals(2L, add.value());
    }

    @Test
    void testUnpackOpAcceptsAnyFieldOrder() throws IOException {
        try (var packer = MessagePack.newDefaultBufferPacker()) {
            packer.packArrayHeader(1);
            packer.packMapHeader(4);
            packer.packString("value"); packer.packString("alice");
            packer.packString("attr"); packer.packString("name");
            packer.packString("entity");
            packer.packMapHeader(2);
            packer.packString("id"); packer.packLong(42L);
            packer.packString("kind"); packer.packString("id");
            packer.packString("kind"); packer.packString("add");

            try (var unpacker = MessagePack.newDefaultUnpacker(packer.toByteArray())) {
                var result = TxOpCodec.unpackOps(unpacker);
                assertEquals(1, result.size());
                var add = (TxOp.Add) result.get(0);
                assertEquals(new EntityRef.Id(42), add.entity());
                assertEquals(Keyword.intern("name"), add.attribute());
                assertEquals("alice", add.value());
            }
        }
    }
}
