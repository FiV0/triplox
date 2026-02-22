package io.triplox.client;

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
        var doc = new TreeMap<String, Object>();
        doc.put("db/id", 1L);
        doc.put("name", "alice");
        var result = roundtrip(List.of(new TxOp.Put(doc)));
        assertEquals(1, result.size());
        var put = (TxOp.Put) result.get(0);
        assertEquals(1L, put.document().get("db/id"));
        assertEquals("alice", put.document().get("name"));
    }

    @Test
    void testAdd() throws IOException {
        var result = roundtrip(List.of(new TxOp.Add(42, "email", "test@example.com")));
        assertEquals(1, result.size());
        var add = (TxOp.Add) result.get(0);
        assertEquals(42, add.entity());
        assertEquals("email", add.attribute());
        assertEquals("test@example.com", add.value());
    }

    @Test
    void testRetract() throws IOException {
        var result = roundtrip(List.of(new TxOp.Retract(42, "email", "old@example.com")));
        assertEquals(1, result.size());
        var ret = (TxOp.Retract) result.get(0);
        assertEquals(42, ret.entity());
        assertEquals("email", ret.attribute());
        assertEquals("old@example.com", ret.value());
    }

    @Test
    void testDelete() throws IOException {
        var result = roundtrip(List.of(new TxOp.Delete(99)));
        assertEquals(1, result.size());
        assertEquals(new TxOp.Delete(99), result.get(0));
    }

    @Test
    void testErase() throws IOException {
        var result = roundtrip(List.of(new TxOp.Erase(100)));
        assertEquals(1, result.size());
        assertEquals(new TxOp.Erase(100), result.get(0));
    }

    @Test
    void testMultipleOps() throws IOException {
        var doc = new TreeMap<String, Object>();
        doc.put("db/id", 1L);
        doc.put("name", "bob");

        var ops = List.<TxOp>of(
                new TxOp.Put(doc),
                new TxOp.Add(1, "age", 30L),
                new TxOp.Retract(1, "name", "old-bob"),
                new TxOp.Delete(99),
                new TxOp.Erase(100)
        );
        var result = roundtrip(ops);
        assertEquals(5, result.size());
        assertInstanceOf(TxOp.Put.class, result.get(0));
        assertInstanceOf(TxOp.Add.class, result.get(1));
        assertInstanceOf(TxOp.Retract.class, result.get(2));
        assertInstanceOf(TxOp.Delete.class, result.get(3));
        assertInstanceOf(TxOp.Erase.class, result.get(4));
    }
}
