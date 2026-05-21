package xyz.triplox.client;

import java.io.IOException;
import java.util.List;

/**
 * Handle to an open DB read basis on the server.
 *
 * <p>Obtained via {@link TriploxNode#openDb()}. Provides query execution
 * and must be closed when no longer needed to release server resources.</p>
 */
public class Db implements AutoCloseable {
    private final TriploxNode node;
    private final int dbId;
    private final long txEid;

    Db(TriploxNode node, int dbId, long txEid) {
        this.node = node;
        this.dbId = dbId;
        this.txEid = txEid;
    }

    int dbId() { return dbId; }
    public long txEid() { return txEid; }

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

    /**
     * Release this DB handle on the server.
     */
    @Override
    public void close() throws IOException {
        node.closeDbInternal(this);
    }
}
