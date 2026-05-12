package io.triplox.client;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/**
 * HTTP/2 client for connecting to a Triplox server.
 *
 * <p>Thread-safe. The underlying {@link HttpClient} handles connection pooling
 * and HTTP/2 multiplexing, so a single {@code TriploxNode} can be shared
 * across threads.</p>
 */
public class TriploxNode implements AutoCloseable {
    private final HttpClient httpClient;
    private final String baseUrl;

    private static final String CONTENT_TYPE = "application/vnd.triplox+msgpack";

    private TriploxNode(HttpClient httpClient, String baseUrl) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
    }

    /**
     * Connect to a Triplox HTTP server.
     */
    public static TriploxNode connect(String host, int port) throws IOException {
        var client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .build();
        String url = "http://" + host + ":" + port;
        return new TriploxNode(client, url);
    }

    /**
     * Open a DB read handle at the latest indexed transaction.
     */
    public Db openDb() throws IOException {
        return openDbInternal(null);
    }

    /**
     * Open a DB read handle at a specific indexed transaction basis.
     */
    public Db openDbAsOf(TxBasis basis) throws IOException {
        return openDbInternal(basis);
    }

    private Db openDbInternal(TxBasis basis) throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(basis);
        byte[] responseBody = postBinary("/db/open", body);
        var opened = WireCodec.decodeDbOpened(responseBody);
        return new Db(this, opened.dbId(), opened.txEid());
    }

    /**
     * Release a previously opened DB read handle.
     */
    void closeDbInternal(Db db) throws IOException {
        deleteBinary("/db/" + db.dbId());
    }

    /**
     * Execute a Datalog query against an open DB read handle.
     */
    QueryResult queryInternal(Db db, String edn) throws IOException {
        return queryInternal(db, edn, List.of());
    }

    /**
     * Execute a Datalog query with input binding arguments.
     */
    QueryResult queryInternal(Db db, String edn, List<QueryArg> args) throws IOException {
        byte[] body = WireCodec.encodeQueryBody(edn, args);
        byte[] responseBody = postBinary("/db/" + db.dbId() + "/query", body);
        return WireCodec.decodeQueryResponse(responseBody);
    }

    /**
     * Submit a fire-and-forget transaction.
     */
    public TxKey submitTx(List<TxOp> ops) throws IOException {
        byte[] body = WireCodec.encodeExecuteBody(ops);
        byte[] responseBody = postBinary("/tx/submit", body);
        var txKey = WireCodec.decodeTxKey(responseBody);
        return new TxKey(txKey.txId(), txKey.systemTime());
    }

    /**
     * Execute a transaction and wait for indexing.
     */
    public TxResult executeTx(List<TxOp> ops) throws IOException {
        byte[] body = WireCodec.encodeExecuteBody(ops);
        byte[] responseBody = postBinary("/tx/execute", body);
        var txResult = WireCodec.decodeTxResult(responseBody);
        return new TxResult(txResult.status(), txResult.txId(),
                txResult.systemTime(), txResult.txEid(), txResult.errorMessage());
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
     * Close the HTTP client.
     */
    @Override
    public void close() {
        httpClient.close();
    }

    // ---------------------------------------------------------------
    // Internal HTTP helpers
    // ---------------------------------------------------------------

    private byte[] postBinary(String path, byte[] body) throws IOException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("Content-Type", CONTENT_TYPE)
                .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                .build();
        return sendAndCheck(request);
    }

    private byte[] deleteBinary(String path) throws IOException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .DELETE()
                .build();
        return sendAndCheck(request);
    }

    private byte[] sendAndCheck(HttpRequest request) throws IOException {
        HttpResponse<byte[]> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted", e);
        }

        int status = response.statusCode();
        if (status >= 200 && status < 300) {
            return response.body();
        }

        // Try to decode binary ErrorResponse from the body
        byte[] responseBody = response.body();
        if (responseBody != null && responseBody.length > 0) {
            try {
                var err = WireCodec.decodeErrorResponse(responseBody);
                throw new TriploxException(err.severity(), err.code(),
                        err.message(), err.detail(), err.hint());
            } catch (TriploxException e) {
                throw e;
            } catch (Exception ignored) {
                // Fall through to generic error
            }
        }
        throw new IOException("HTTP error " + status + ": " + new String(responseBody));
    }

}
