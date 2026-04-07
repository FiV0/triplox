package io.triplox.client;

import clojure.lang.Keyword;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.List;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class TxOpCodecTest {

    private List<TxOp> roundtrip(List<TxOp> ops) throws IOException {
        var baos = new ByteArrayOutputStream();
        var dos = new DataOutputStream(baos);
        TxOpCodec.encode(dos, ops);
        dos.flush();

        var bin = new ByteArrayInputStream(baos.toByteArray());
        var dis = new DataInputStream(bin);
        return TxOpCodec.decode(dis);
    }

    @Test
    void testPut() throws IOException {
        var doc = new TreeMap<Keyword, TxValue>();
        doc.put(Keyword.intern("db", "id"), new TxValue.Ref(new EntityRef.Id(1)));
        doc.put(Keyword.intern("name"), new TxValue.Data("alice"));
        var result = roundtrip(List.of(new TxOp.Put(doc)));
        assertEquals(1, result.size());
        var put = (TxOp.Put) result.get(0);
        var idVal = (TxValue.Ref) put.document().get(Keyword.intern("db", "id"));
        assertEquals(1L, ((EntityRef.Id) idVal.ref()).id());
        var nameVal = (TxValue.Data) put.document().get(Keyword.intern("name"));
        assertEquals("alice", nameVal.value());
    }

    @Test
    void testAdd() throws IOException {
        var result = roundtrip(List.of(new TxOp.Add(
                new EntityRef.Id(42),
                Keyword.intern("email"),
                new TxValue.Data("test@example.com"))));
        assertEquals(1, result.size());
        var add = (TxOp.Add) result.get(0);
        assertEquals(42, ((EntityRef.Id) add.entity()).id());
        assertEquals(Keyword.intern("email"), add.attribute());
        assertEquals("test@example.com", ((TxValue.Data) add.value()).value());
    }

    @Test
    void testRetract() throws IOException {
        var result = roundtrip(List.of(new TxOp.Retract(
                new EntityRef.Id(42),
                Keyword.intern("email"),
                new TxValue.Data("old@example.com"))));
        assertEquals(1, result.size());
        var ret = (TxOp.Retract) result.get(0);
        assertEquals(42, ((EntityRef.Id) ret.entity()).id());
        assertEquals(Keyword.intern("email"), ret.attribute());
        assertEquals("old@example.com", ((TxValue.Data) ret.value()).value());
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
        var doc = new TreeMap<Keyword, TxValue>();
        doc.put(Keyword.intern("db", "id"), new TxValue.Ref(new EntityRef.Id(1)));
        doc.put(Keyword.intern("name"), new TxValue.Data("bob"));

        var ops = List.<TxOp>of(
                new TxOp.Put(doc),
                new TxOp.Add(new EntityRef.Id(1), Keyword.intern("age"), new TxValue.Data(30L)),
                new TxOp.Retract(new EntityRef.Id(1), Keyword.intern("name"), new TxValue.Data("old-bob")),
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
    void testTxValueRef() throws IOException {
        var result = roundtrip(List.of(new TxOp.Add(
                new EntityRef.Id(1),
                Keyword.intern("friend"),
                new TxValue.Ref(new EntityRef.Id(2)))));
        assertEquals(1, result.size());
        var add = (TxOp.Add) result.get(0);
        var ref = (TxValue.Ref) add.value();
        assertEquals(new EntityRef.Id(2), ref.ref());
    }
}
