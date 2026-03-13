package io.triplox.client;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Manages a TCP connection to a Triplox server.
 *
 * <p>Not thread-safe. The wire protocol is serial (one operation at a time),
 * so callers needing concurrency should use separate connections.</p>
 */
public class TriploxNode implements AutoCloseable {
    private final Socket socket;
    private final BufferedOutputStream out;
    private final BufferedInputStream in;

    private TriploxNode(Socket socket) throws IOException {
        this.socket = socket;
        this.out = new BufferedOutputStream(socket.getOutputStream());
        this.in = new BufferedInputStream(socket.getInputStream());
    }

    /**
     * Connect to a Triplox server and perform the startup handshake.
     */
    public static TriploxNode connect(String host, int port) throws IOException {
        return connect(host, port, Map.of());
    }

    /**
     * Connect to a Triplox server with custom startup parameters.
     */
    public static TriploxNode connect(String host, int port, Map<String, String> params) throws IOException {
        Socket socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        var node = new TriploxNode(socket);
        try {
            node.performStartup(params);
            return node;
        } catch (Exception e) {
            node.close();
            throw e;
        }
    }

    /**
     * Open a DB snapshot at the latest indexed transaction.
     */
    public Db openDb() throws IOException {
        return openDb(null);
    }

    /**
     * Open a DB snapshot at a specific transaction ID.
     */
    public Db openDb(Long basisTxId) throws IOException {
        WireCodec.writeOpenDb(out, basisTxId);
        out.flush();

        BackendMessage msg = readExpecting("DbOpened");
        if (msg instanceof BackendMessage.DbOpened opened) {
            expectReadyForQuery();
            return new Db(this, opened.dbId(), opened.txId());
        }
        throw unexpectedMessage("DbOpened", msg);
    }

    /**
     * Release a previously opened DB snapshot.
     */
    void closeDbInternal(Db db) throws IOException {
        WireCodec.writeCloseDb(out, db.dbId());
        out.flush();

        BackendMessage msg = readExpecting("DbClosed");
        if (msg instanceof BackendMessage.DbClosed closed) {
            if (closed.dbId() != db.dbId()) {
                throw new IOException("DbClosed for wrong dbId: expected " + db.dbId() + ", got " + closed.dbId());
            }
            expectReadyForQuery();
            return;
        }
        throw unexpectedMessage("DbClosed", msg);
    }

    /**
     * Execute a Datalog query against an open DB snapshot.
     */
    QueryResult queryInternal(Db db, String edn) throws IOException {
        WireCodec.writeQuery(out, edn, db.dbId());
        out.flush();

        // Read RowDescription
        BackendMessage msg = readExpecting("RowDescription");
        if (!(msg instanceof BackendMessage.RowDescription rowDesc)) {
            throw unexpectedMessage("RowDescription", msg);
        }

        // Read DataRows until ReadyForQuery
        var rows = new ArrayList<List<Object>>();
        while (true) {
            msg = readExpecting("DataRow or ReadyForQuery");
            if (msg instanceof BackendMessage.DataRow dataRow) {
                rows.add(dataRow.values());
            } else if (msg instanceof BackendMessage.ReadyForQuery) {
                break;
            } else {
                throw unexpectedMessage("DataRow or ReadyForQuery", msg);
            }
        }

        return new QueryResult(rowDesc.columns(), rows);
    }

    /**
     * Submit a fire-and-forget transaction (await_indexing=false).
     */
    public TxKeyResult submitTx(List<TxOp> ops) throws IOException {
        WireCodec.writeExecute(out, ops, false);
        out.flush();

        BackendMessage msg = readExpecting("TxKey");
        if (msg instanceof BackendMessage.TxKey txKey) {
            expectReadyForQuery();
            return new TxKeyResult(txKey.txId(), txKey.systemTime());
        }
        throw unexpectedMessage("TxKey", msg);
    }

    /**
     * Execute a transaction and wait for indexing (await_indexing=true).
     */
    public TxResultValue executeTx(List<TxOp> ops) throws IOException {
        WireCodec.writeExecute(out, ops, true);
        out.flush();

        BackendMessage msg = readExpecting("TxResult");
        if (msg instanceof BackendMessage.TxResult txResult) {
            expectReadyForQuery();
            return new TxResultValue(txResult.status(), txResult.txId(),
                    txResult.systemTime(), txResult.seqNum(), txResult.errorMessage());
        }
        throw unexpectedMessage("TxResult", msg);
    }

    /**
     * Stub — subscription not yet supported.
     */
    public void subscribe(Db db, String edn) {
        throw new UnsupportedOperationException("subscribe is not yet supported");
    }

    /**
     * Stub — subscription not yet supported.
     */
    public void unsubscribe() {
        throw new UnsupportedOperationException("unsubscribe is not yet supported");
    }

    /**
     * Graceful connection close.
     */
    @Override
    public void close() throws IOException {
        try {
            if (!socket.isClosed()) {
                WireCodec.writeTerminate(out);
                out.flush();
            }
        } catch (IOException ignored) {
            // Best-effort terminate
        } finally {
            socket.close();
        }
    }

    // ---------------------------------------------------------------
    // Internal
    // ---------------------------------------------------------------

    private void performStartup(Map<String, String> params) throws IOException {
        WireCodec.writeStartup(out, params);
        out.flush();

        BackendMessage msg = readExpecting("AuthenticationOk");
        if (!(msg instanceof BackendMessage.AuthenticationOk)) {
            throw unexpectedMessage("AuthenticationOk", msg);
        }

        expectReadyForQuery();
    }

    private void expectReadyForQuery() throws IOException {
        BackendMessage msg = readExpecting("ReadyForQuery");
        if (msg instanceof BackendMessage.ReadyForQuery rfq) {
            if (rfq.status() != MessageTypes.STATUS_IDLE && rfq.status() != MessageTypes.STATUS_SUBSCRIBED) {
                throw new IOException("Unexpected ReadyForQuery status: 0x" +
                        Integer.toHexString(rfq.status() & 0xFF));
            }
            return;
        }
        throw unexpectedMessage("ReadyForQuery", msg);
    }

    private BackendMessage readExpecting(String context) throws IOException {
        BackendMessage msg = WireCodec.readBackendMessage(in);
        if (msg instanceof BackendMessage.ErrorResponse err) {
            var ex = new TriploxException(err.severity(), err.code(),
                    err.message(), err.detail(), err.hint());
            if (ex.isFatal()) {
                throw ex;
            }
            // Non-fatal: ReadyForQuery follows, but we throw for the caller
            // Read and discard the ReadyForQuery
            WireCodec.readBackendMessage(in);
            throw ex;
        }
        return msg;
    }

    private static IOException unexpectedMessage(String expected, BackendMessage actual) {
        return new IOException("Expected " + expected + " but got " + actual.getClass().getSimpleName());
    }
}
