package io.triplox.client;

import clojure.lang.Keyword;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static io.triplox.client.MessageTypes.*;

/**
 * Encodes and decodes TxOp values on the wire.
 */
public final class TxOpCodec {
    private TxOpCodec() {}

    public static void encode(DataOutputStream out, List<TxOp> ops) throws IOException {
        out.writeInt(ops.size());
        for (TxOp op : ops) {
            encodeOp(out, op);
        }
    }

    public static List<TxOp> decode(DataInputStream in) throws IOException {
        int count = in.readInt();
        var ops = new ArrayList<TxOp>(count);
        for (int i = 0; i < count; i++) {
            ops.add(decodeOp(in));
        }
        return ops;
    }

    // ---------------------------------------------------------------
    // EntityRef
    // ---------------------------------------------------------------

    static void encodeEntityRef(DataOutputStream out, EntityRef ref) throws IOException {
        switch (ref) {
            case EntityRef.Id id -> {
                out.writeByte(ENTITY_REF_ID);
                out.writeLong(id.id());
            }
            case EntityRef.TempId tempId -> {
                out.writeByte(ENTITY_REF_TEMPID);
                DataTypeCodec.encodeString(out, tempId.tempId());
            }
            case EntityRef.Ident ident -> {
                out.writeByte(ENTITY_REF_IDENT);
                DataTypeCodec.encodeString(out, ident.ident().toString());
            }
            case EntityRef.LookupRef lr -> {
                out.writeByte(ENTITY_REF_LOOKUP);
                DataTypeCodec.encodeString(out, lr.attr().toString());
                DataTypeCodec.encode(out, lr.value());
            }
        }
    }

    static EntityRef decodeEntityRef(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case ENTITY_REF_ID -> new EntityRef.Id(in.readLong());
            case ENTITY_REF_TEMPID -> new EntityRef.TempId(DataTypeCodec.decodeString(in));
            case ENTITY_REF_IDENT -> new EntityRef.Ident(DataTypeCodec.parseKeyword(DataTypeCodec.decodeString(in)));
            case ENTITY_REF_LOOKUP -> {
                var attr = DataTypeCodec.parseKeyword(DataTypeCodec.decodeString(in));
                var value = DataTypeCodec.decode(in);
                yield new EntityRef.LookupRef(attr, value);
            }
            default -> throw new IOException("Unknown EntityRef tag: 0x" + Integer.toHexString(tag & 0xFF));
        };
    }

    // ---------------------------------------------------------------
    // TxOp
    // ---------------------------------------------------------------

    static void encodeOp(DataOutputStream out, TxOp op) throws IOException {
        switch (op) {
            case TxOp.Put put -> {
                out.writeByte(TXOP_PUT);
                encodeKeywordObjectMap(out, put.document());
            }
            case TxOp.Add add -> {
                out.writeByte(TXOP_ADD);
                encodeEntityRef(out, add.entity());
                DataTypeCodec.encodeString(out, add.attribute().toString());
                DataTypeCodec.encode(out, add.value());
            }
            case TxOp.Retract ret -> {
                out.writeByte(TXOP_RETRACT);
                encodeEntityRef(out, ret.entity());
                DataTypeCodec.encodeString(out, ret.attribute().toString());
                DataTypeCodec.encode(out, ret.value());
            }
            case TxOp.Delete del -> {
                out.writeByte(TXOP_DELETE);
                encodeEntityRef(out, del.entity());
            }
            case TxOp.Erase erase -> {
                out.writeByte(TXOP_ERASE);
                encodeEntityRef(out, erase.entity());
            }
        }
    }

    static TxOp decodeOp(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case TXOP_PUT -> {
                var map = decodeKeywordObjectMap(in);
                yield new TxOp.Put(map);
            }
            case TXOP_ADD -> {
                var entity = decodeEntityRef(in);
                var attribute = DataTypeCodec.parseKeyword(DataTypeCodec.decodeString(in));
                var value = DataTypeCodec.decode(in);
                yield new TxOp.Add(entity, attribute, value);
            }
            case TXOP_RETRACT -> {
                var entity = decodeEntityRef(in);
                var attribute = DataTypeCodec.parseKeyword(DataTypeCodec.decodeString(in));
                var value = DataTypeCodec.decode(in);
                yield new TxOp.Retract(entity, attribute, value);
            }
            case TXOP_DELETE -> new TxOp.Delete(decodeEntityRef(in));
            case TXOP_ERASE -> new TxOp.Erase(decodeEntityRef(in));
            default -> throw new IOException("Unknown TxOp tag: " + (tag & 0xFF));
        };
    }

    // ---------------------------------------------------------------
    // Keyword → Object map
    // ---------------------------------------------------------------

    private static void encodeKeywordObjectMap(DataOutputStream out, Map<Keyword, Object> map) throws IOException {
        var sorted = new TreeMap<>(map);
        out.writeInt(sorted.size());
        for (var entry : sorted.entrySet()) {
            DataTypeCodec.encodeString(out, entry.getKey().toString());
            DataTypeCodec.encode(out, entry.getValue());
        }
    }

    private static Map<Keyword, Object> decodeKeywordObjectMap(DataInputStream in) throws IOException {
        int count = in.readInt();
        var map = new TreeMap<Keyword, Object>();
        for (int i = 0; i < count; i++) {
            var key = DataTypeCodec.parseKeyword(DataTypeCodec.decodeString(in));
            var value = DataTypeCodec.decode(in);
            map.put(key, value);
        }
        return map;
    }
}
