package io.triplox.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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

    static void encodeOp(DataOutputStream out, TxOp op) throws IOException {
        switch (op) {
            case TxOp.Put put -> {
                out.writeByte(TXOP_PUT);
                DataTypeCodec.encodeDataTypeMap(out, put.document());
            }
            case TxOp.Add add -> {
                out.writeByte(TXOP_ADD);
                encodeTriple(out, add.entity(), add.attribute(), add.value());
            }
            case TxOp.Retract ret -> {
                out.writeByte(TXOP_RETRACT);
                encodeTriple(out, ret.entity(), ret.attribute(), ret.value());
            }
            case TxOp.Delete del -> {
                out.writeByte(TXOP_DELETE);
                out.writeLong(del.entity());
            }
            case TxOp.Erase erase -> {
                out.writeByte(TXOP_ERASE);
                out.writeLong(erase.entity());
            }
        }
    }

    static TxOp decodeOp(DataInputStream in) throws IOException {
        byte tag = in.readByte();
        return switch (tag) {
            case TXOP_PUT -> {
                Map<String, Object> doc = DataTypeCodec.decodeDataTypeMap(in);
                yield new TxOp.Put(doc);
            }
            case TXOP_ADD -> {
                long entity = in.readLong();
                String attribute = DataTypeCodec.decodeString(in);
                Object value = DataTypeCodec.decode(in);
                yield new TxOp.Add(entity, attribute, value);
            }
            case TXOP_RETRACT -> {
                long entity = in.readLong();
                String attribute = DataTypeCodec.decodeString(in);
                Object value = DataTypeCodec.decode(in);
                yield new TxOp.Retract(entity, attribute, value);
            }
            case TXOP_DELETE -> new TxOp.Delete(in.readLong());
            case TXOP_ERASE -> new TxOp.Erase(in.readLong());
            default -> throw new IOException("Unknown TxOp tag: " + (tag & 0xFF));
        };
    }

    private static void encodeTriple(DataOutputStream out, long entity, String attribute, Object value) throws IOException {
        out.writeLong(entity);
        DataTypeCodec.encodeString(out, attribute);
        DataTypeCodec.encode(out, value);
    }
}
