package xyz.triplox.client;

import java.io.IOException;
import java.util.List;

/**
 * Immutable DB read basis.
 *
 * <p>Obtained via {@link TriploxNode#openDb()}. Provides query execution.</p>
 */
public class Db {
    private final TriploxNode node;
    private final TxKey basis;

    Db(TriploxNode node, TxKey basis) {
        this.node = node;
        this.basis = basis;
    }

    public TxKey basis() { return basis; }

    /**
     * Execute a Datalog query against this DB read basis.
     */
    public List<List<Object>> query(String edn) throws IOException {
        return node.queryInternal(this, edn).rows();
    }

    /**
     * Execute a Datalog query with input binding arguments.
     */
    public List<List<Object>> query(String edn, List<QueryArg> args) throws IOException {
        return node.queryInternal(this, edn, args).rows();
    }
}
