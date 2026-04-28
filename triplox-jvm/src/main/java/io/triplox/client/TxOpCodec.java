package io.triplox.client;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import clojure.lang.Keyword;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

/**
 * MessagePack codec for {@link TxOp} and {@link EntityRef} values.
 *
 * <p>Tagged unions use {@code {"kind": "<variant>", ...fields}} maps.
 * See {@code design/WIRE_PROTOCOL.md}.</p>
 */
public final class TxOpCodec {
    private TxOpCodec() {}

    // ---------------------------------------------------------------
    // List<TxOp>
    // ---------------------------------------------------------------

    public static void packOps(MessagePacker packer, List<TxOp> ops) throws IOException {
        packer.packArrayHeader(ops.size());
        for (TxOp op : ops) {
            packOp(packer, op);
        }
    }

    public static List<TxOp> unpackOps(MessageUnpacker unpacker) throws IOException {
        int count = unpacker.unpackArrayHeader();
        var ops = new java.util.ArrayList<TxOp>(count);
        for (int i = 0; i < count; i++) {
            ops.add(unpackOp(unpacker));
        }
        return ops;
    }

    // ---------------------------------------------------------------
    // EntityRef
    // ---------------------------------------------------------------

    public static void packEntityRef(MessagePacker packer, EntityRef ref) throws IOException {
        switch (ref) {
            case EntityRef.Id id -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("id");
                packer.packString("id"); packer.packLong(id.id());
            }
            case EntityRef.TempId temp -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("temp");
                packer.packString("temp"); packer.packString(temp.tempId());
            }
            case EntityRef.Ident ident -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("ident");
                packer.packString("ident"); packer.packString(DataTypeCodec.keywordToWire(ident.ident()));
            }
            case EntityRef.LookupRef lr -> {
                packer.packMapHeader(3);
                packer.packString("kind"); packer.packString("lookup");
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordToWire(lr.attr()));
                packer.packString("value"); DataTypeCodec.pack(packer, lr.value());
            }
        }
    }

    public static EntityRef unpackEntityRef(MessageUnpacker unpacker) throws IOException {
        Map<String, Object> map = unpackTaggedMap(unpacker);
        String kind = takeString(map, "kind");
        return switch (kind) {
            case "id" -> new EntityRef.Id((Long) requireField(map, "id"));
            case "temp" -> new EntityRef.TempId(takeString(map, "temp"));
            case "ident" -> new EntityRef.Ident(DataTypeCodec.keywordFromWire(takeString(map, "ident")));
            case "lookup" -> {
                Keyword attr = DataTypeCodec.keywordFromWire(takeString(map, "attr"));
                Object value = requireField(map, "value");
                yield new EntityRef.LookupRef(attr, value);
            }
            default -> throw new IOException("Unknown EntityRef kind: " + kind);
        };
    }

    // ---------------------------------------------------------------
    // TxOp
    // ---------------------------------------------------------------

    public static void packOp(MessagePacker packer, TxOp op) throws IOException {
        switch (op) {
            case TxOp.Put put -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("put");
                packer.packString("doc");
                Map<Keyword, Object> doc = put.document();
                packer.packMapHeader(doc.size());
                for (var entry : doc.entrySet()) {
                    packer.packString(DataTypeCodec.keywordToWire(entry.getKey()));
                    DataTypeCodec.pack(packer, entry.getValue());
                }
            }
            case TxOp.Add add -> {
                packer.packMapHeader(4);
                packer.packString("kind"); packer.packString("add");
                packer.packString("entity"); packEntityRef(packer, add.entity());
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordToWire(add.attribute()));
                packer.packString("value"); DataTypeCodec.pack(packer, add.value());
            }
            case TxOp.Retract ret -> {
                packer.packMapHeader(4);
                packer.packString("kind"); packer.packString("retract");
                packer.packString("entity"); packEntityRef(packer, ret.entity());
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordToWire(ret.attribute()));
                packer.packString("value"); DataTypeCodec.pack(packer, ret.value());
            }
            case TxOp.Delete del -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("delete");
                packer.packString("entity"); packEntityRef(packer, del.entity());
            }
            case TxOp.Erase erase -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("erase");
                packer.packString("entity"); packEntityRef(packer, erase.entity());
            }
        }
    }

    public static TxOp unpackOp(MessageUnpacker unpacker) throws IOException {
        int count = unpacker.unpackMapHeader();
        // Parse the map. We need special-case handling for "entity" (an EntityRef map),
        // so we can't fully buffer with `unpack(...)` for those fields. Read kind first
        // then dispatch.
        String kind = null;
        Map<String, Object> simple = new LinkedHashMap<>();
        EntityRef entity = null;
        // Two-pass: collect field names+values, but EntityRef must be unpacked inline.
        // Easier: read the first key (must be "kind"), then dispatch by kind and
        // read the rest in declared order.
        if (count < 1) {
            throw new IOException("TxOp map must contain at least \"kind\"");
        }
        String firstKey = unpacker.unpackString();
        if (!"kind".equals(firstKey)) {
            throw new IOException("TxOp map first key must be \"kind\", got: " + firstKey);
        }
        kind = unpacker.unpackString();
        return switch (kind) {
            case "put" -> readPut(unpacker, count - 1);
            case "add" -> readAddOrRetract(unpacker, count - 1, /*retract=*/false);
            case "retract" -> readAddOrRetract(unpacker, count - 1, /*retract=*/true);
            case "delete" -> {
                EntityRef e = readEntityField(unpacker, count - 1);
                yield new TxOp.Delete(e);
            }
            case "erase" -> {
                EntityRef e = readEntityField(unpacker, count - 1);
                yield new TxOp.Erase(e);
            }
            default -> throw new IOException("Unknown TxOp kind: " + kind);
        };
    }

    private static TxOp.Put readPut(MessageUnpacker unpacker, int remainingFields) throws IOException {
        Map<Keyword, Object> doc = null;
        for (int i = 0; i < remainingFields; i++) {
            String key = unpacker.unpackString();
            switch (key) {
                case "doc" -> {
                    int docSize = unpacker.unpackMapHeader();
                    doc = new LinkedHashMap<>();
                    for (int j = 0; j < docSize; j++) {
                        String kStr = unpacker.unpackString();
                        Object v = DataTypeCodec.unpack(unpacker);
                        doc.put(DataTypeCodec.keywordFromWire(kStr), v);
                    }
                }
                default -> unpacker.skipValue();
            }
        }
        if (doc == null) throw new IOException("Put missing \"doc\" field");
        return new TxOp.Put(doc);
    }

    private static TxOp readAddOrRetract(MessageUnpacker unpacker, int remainingFields, boolean retract)
            throws IOException {
        EntityRef entity = null;
        Keyword attr = null;
        Object value = null;
        boolean valueSet = false;
        for (int i = 0; i < remainingFields; i++) {
            String key = unpacker.unpackString();
            switch (key) {
                case "entity" -> entity = unpackEntityRef(unpacker);
                case "attr" -> attr = DataTypeCodec.keywordFromWire(unpacker.unpackString());
                case "value" -> { value = DataTypeCodec.unpack(unpacker); valueSet = true; }
                default -> unpacker.skipValue();
            }
        }
        if (entity == null || attr == null || !valueSet) {
            throw new IOException((retract ? "Retract" : "Add") + " missing required field(s)");
        }
        return retract ? new TxOp.Retract(entity, attr, value) : new TxOp.Add(entity, attr, value);
    }

    private static EntityRef readEntityField(MessageUnpacker unpacker, int remainingFields)
            throws IOException {
        EntityRef entity = null;
        for (int i = 0; i < remainingFields; i++) {
            String key = unpacker.unpackString();
            if ("entity".equals(key)) {
                entity = unpackEntityRef(unpacker);
            } else {
                unpacker.skipValue();
            }
        }
        if (entity == null) throw new IOException("missing \"entity\" field");
        return entity;
    }

    // ---------------------------------------------------------------
    // EntityRef map decode helpers
    // ---------------------------------------------------------------

    private static Map<String, Object> unpackTaggedMap(MessageUnpacker unpacker) throws IOException {
        int count = unpacker.unpackMapHeader();
        var map = new LinkedHashMap<String, Object>();
        for (int i = 0; i < count; i++) {
            String key = unpacker.unpackString();
            map.put(key, DataTypeCodec.unpack(unpacker));
        }
        return map;
    }

    private static String takeString(Map<String, Object> map, String name) throws IOException {
        Object v = requireField(map, name);
        if (v instanceof String s) return s;
        throw new IOException("field " + name + " expected String, got " + v.getClass().getName());
    }

    private static Object requireField(Map<String, Object> map, String name) throws IOException {
        Object v = map.get(name);
        if (v == null) throw new IOException("missing field: " + name);
        return v;
    }
}
