package io.triplox.client;

import java.io.IOException;

/**
 * Handle to an open DB snapshot on the server.
 *
 * <p>Obtained via {@link TriploxNode#openDb()}. Provides query execution
 * and must be closed when no longer needed to release server resources.</p>
 */
public class DbHandle implements AutoCloseable {
    private final TriploxNode node;
    private final int dbId;
    private final long txId;

    DbHandle(TriploxNode node, int dbId, long txId) {
        this.node = node;
        this.dbId = dbId;
        this.txId = txId;
    }

    public int dbId() { return dbId; }
    public long txId() { return txId; }

    /**
     * Execute a Datalog query against this DB snapshot.
     */
    public QueryResult query(String edn) throws IOException {
        return node.queryInternal(this, edn);
    }

    /**
     * Release this DB snapshot on the server.
     */
    @Override
    public void close() throws IOException {
        node.closeDbInternal(this);
    }
}
