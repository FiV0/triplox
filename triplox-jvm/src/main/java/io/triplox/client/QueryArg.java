package io.triplox.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import static io.triplox.client.MessageTypes.*;

/**
 * A query input argument corresponding to an {@code :in} binding form.
 */
public sealed interface QueryArg {

    record Scalar(Object value) implements QueryArg {}

    // TODO: Collection, Tuple, Relation are not yet supported in the EDN parser
    record Collection(List<Object> values) implements QueryArg {}
    record Tuple(List<Object> values) implements QueryArg {}
    record Relation(List<List<Object>> rows) implements QueryArg {}

    static void encode(DataOutputStream out, QueryArg arg) throws IOException {
        switch (arg) {
            case Scalar s -> {
                out.writeByte(QUERY_ARG_SCALAR);
                DataTypeCodec.encode(out, s.value());
            }
            case Collection c -> {
                out.writeByte(QUERY_ARG_COLLECTION);
                DataTypeCodec.encodeDataTypeVec(out, c.values());
            }
            case Tuple t -> {
                out.writeByte(QUERY_ARG_TUPLE);
                DataTypeCodec.encodeDataTypeVec(out, t.values());
            }
            case Relation r -> {
                out.writeByte(QUERY_ARG_RELATION);
                out.writeInt(r.rows().size());
                for (List<Object> row : r.rows()) {
                    DataTypeCodec.encodeDataTypeVec(out, row);
                }
            }
        }
    }

    static void encodeArgs(DataOutputStream out, List<QueryArg> args) throws IOException {
        out.writeInt(args.size());
        for (QueryArg arg : args) {
            encode(out, arg);
        }
    }
}
