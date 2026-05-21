package xyz.triplox.client;

import java.io.IOException;
import java.util.List;

import org.msgpack.core.MessagePacker;

/**
 * A query input argument corresponding to an {@code :in} binding form.
 *
 * <p>Wire shape (msgpack): {@code {"kind": "<variant>", ...fields}}.</p>
 */
public sealed interface QueryArg {

    record Scalar(Object value) implements QueryArg {}
    record Collection(List<Object> values) implements QueryArg {}
    record Tuple(List<Object> values) implements QueryArg {}
    record Relation(List<List<Object>> rows) implements QueryArg {}

    static void pack(MessagePacker packer, QueryArg arg) throws IOException {
        switch (arg) {
            case Scalar s -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("scalar");
                packer.packString("value"); DataTypeCodec.pack(packer, s.value());
            }
            case Collection c -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("collection");
                packer.packString("values");
                packer.packArrayHeader(c.values().size());
                for (Object v : c.values()) DataTypeCodec.pack(packer, v);
            }
            case Tuple t -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("tuple");
                packer.packString("values");
                packer.packArrayHeader(t.values().size());
                for (Object v : t.values()) DataTypeCodec.pack(packer, v);
            }
            case Relation r -> {
                packer.packMapHeader(2);
                packer.packString("kind"); packer.packString("relation");
                packer.packString("rows");
                packer.packArrayHeader(r.rows().size());
                for (List<Object> row : r.rows()) {
                    packer.packArrayHeader(row.size());
                    for (Object v : row) DataTypeCodec.pack(packer, v);
                }
            }
        }
    }

    static void packAll(MessagePacker packer, List<QueryArg> args) throws IOException {
        packer.packArrayHeader(args.size());
        for (QueryArg a : args) pack(packer, a);
    }
}
