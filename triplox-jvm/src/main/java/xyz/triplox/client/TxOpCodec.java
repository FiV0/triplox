package xyz.triplox.client;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

/**
 * MessagePack codec for {@link TxOp} and {@link EntityRef} values.
 *
 * <p>Tagged unions use {@code {"kind": "<variant>", ...fields}} maps.
 * See {@code design/PROTOCOL.md}.</p>
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
                packer.packString("ident"); packer.packString(DataTypeCodec.keywordStringToWire(ident.ident()));
            }
            case EntityRef.LookupRef lr -> {
                packer.packMapHeader(3);
                packer.packString("kind"); packer.packString("lookup");
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordStringToWire(lr.attr()));
                packer.packString("value"); DataTypeCodec.pack(packer, lr.value());
            }
        }
    }

    public static EntityRef unpackEntityRef(MessageUnpacker unpacker) throws IOException {
        Map<String, Object> map = expectStringMap(DataTypeCodec.unpack(unpacker), "EntityRef");
        return entityRefFromMap(map);
    }

    private static EntityRef entityRefFromMap(Map<String, Object> map) throws IOException {
        String kind = takeString(map, "kind");
        return switch (kind) {
            case "id" -> new EntityRef.Id((Long) requireField(map, "id"));
            case "temp" -> new EntityRef.TempId(takeString(map, "temp"));
            case "ident" -> new EntityRef.Ident(DataTypeCodec.keywordWireToString(takeString(map, "ident")));
            case "lookup" -> {
                String attr = DataTypeCodec.keywordWireToString(takeString(map, "attr"));
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
                Map<String, Object> doc = put.document();
                packer.packMapHeader(doc.size());
                for (var entry : doc.entrySet()) {
                    packer.packString(DataTypeCodec.keywordStringToWire(entry.getKey()));
                    DataTypeCodec.pack(packer, entry.getValue());
                }
            }
            case TxOp.Add add -> {
                packer.packMapHeader(4);
                packer.packString("kind"); packer.packString("add");
                packer.packString("entity"); packEntityRef(packer, add.entity());
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordStringToWire(add.attribute()));
                packer.packString("value"); DataTypeCodec.pack(packer, add.value());
            }
            case TxOp.Retract ret -> {
                packer.packMapHeader(4);
                packer.packString("kind"); packer.packString("retract");
                packer.packString("entity"); packEntityRef(packer, ret.entity());
                packer.packString("attr"); packer.packString(DataTypeCodec.keywordStringToWire(ret.attribute()));
                packer.packString("value"); DataTypeCodec.pack(packer, ret.value());
            }
            case TxOp.RetractEntity retractEntity -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("retractEntity");
                packer.packString("entity"); packEntityRef(packer, retractEntity.entity());
            }
            case TxOp.Erase erase -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("erase");
                packer.packString("entity"); packEntityRef(packer, erase.entity());
            }
        }
    }

    public static TxOp unpackOp(MessageUnpacker unpacker) throws IOException {
        Map<String, Object> map = expectStringMap(DataTypeCodec.unpack(unpacker), "TxOp");
        String kind = takeString(map, "kind");
        return switch (kind) {
            case "put" -> readPut(map);
            case "add" -> readAddOrRetract(map, /*retract=*/false);
            case "retract" -> readAddOrRetract(map, /*retract=*/true);
            case "retractEntity" -> new TxOp.RetractEntity(takeEntityRef(map, "entity"));
            case "erase" -> new TxOp.Erase(takeEntityRef(map, "entity"));
            default -> throw new IOException("Unknown TxOp kind: " + kind);
        };
    }

    private static TxOp.Put readPut(Map<String, Object> map) throws IOException {
        Map<String, Object> rawDoc = expectStringMap(requireField(map, "doc"), "Put.doc");
        Map<String, Object> doc = new LinkedHashMap<>();
        for (var entry : rawDoc.entrySet()) {
            doc.put(DataTypeCodec.keywordWireToString(entry.getKey()), entry.getValue());
        }
        return new TxOp.Put(doc);
    }

    private static TxOp readAddOrRetract(Map<String, Object> map, boolean retract)
            throws IOException {
        EntityRef entity = takeEntityRef(map, "entity");
        String attr = DataTypeCodec.keywordWireToString(takeString(map, "attr"));
        Object value = requireField(map, "value");
        return retract ? new TxOp.Retract(entity, attr, value) : new TxOp.Add(entity, attr, value);
    }

    private static EntityRef takeEntityRef(Map<String, Object> map, String name) throws IOException {
        return entityRefFromMap(expectStringMap(requireField(map, name), name));
    }

    // ---------------------------------------------------------------
    // EntityRef map decode helpers
    // ---------------------------------------------------------------

    private static Map<String, Object> expectStringMap(Object value, String context) throws IOException {
        if (!(value instanceof Map<?, ?> raw)) {
            throw new IOException(context + " expected map, got " + value.getClass().getName());
        }
        var out = new LinkedHashMap<String, Object>();
        for (var entry : raw.entrySet()) {
            if (!(entry.getKey() instanceof String key)) {
                throw new IOException(context + " map key expected String, got " + entry.getKey().getClass().getName());
            }
            out.put(key, entry.getValue());
        }
        return out;
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
