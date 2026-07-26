package xyz.triplox.client;

import java.io.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

/**
 * HTTP/2 client for connecting to a Triplox server.
 *
 * <p>Thread-safe. The underlying {@link OkHttpClient} handles connection pooling
 * and HTTP/2 multiplexing, so a single {@code TriploxNode} can be shared
 * across threads.</p>
 */
public class TriploxNode implements AutoCloseable {
    private final OkHttpClient httpClient;
    private final String baseUrl;

    private static final String CONTENT_TYPE = "application/vnd.triplox+msgpack";
    private static final MediaType CONTENT_MEDIA_TYPE = MediaType.get(CONTENT_TYPE);
    private static final String SUBSCRIBE_PATH = "/db/subscribe";

    private TriploxNode(OkHttpClient httpClient, String baseUrl) {
        this.httpClient = httpClient;
        this.baseUrl = baseUrl;
    }

    static Response disableReadTimeoutForSubscriptions(Interceptor.Chain chain) throws IOException {
        Request request = chain.request();
        if (SUBSCRIBE_PATH.equals(request.url().encodedPath())) {
            return chain.withReadTimeout(0, TimeUnit.MILLISECONDS).proceed(request);
        }
        return chain.proceed(request);
    }

    /**
     * Connect to a Triplox HTTP server.
     */
    public static TriploxNode connect(String host, int port) throws IOException {
        var client = new OkHttpClient.Builder()
                .protocols(List.of(Protocol.H2_PRIOR_KNOWLEDGE))
                .addInterceptor(TriploxNode::disableReadTimeoutForSubscriptions)
                .build();
        return new TriploxNode(client, "http://" + host + ":" + port);
    }

    /**
     * Open a DB value at the latest indexed transaction.
     */
    public Db openDb() throws IOException {
        return openDbInternal(null);
    }

    /**
     * Open a DB value at a specific indexed transaction key.
     */
    public Db openDbAsOf(TxKey txKey) throws IOException {
        return openDbInternal(txKey);
    }

    private Db openDbInternal(TxKey txKey) throws IOException {
        byte[] body = WireCodec.encodeOpenDbBody(txKey);
        byte[] responseBody = postBinary("/db/open", body);
        var opened = WireCodec.decodeTxKey(responseBody);
        return new Db(this, new TxKey(opened.txId(), opened.systemTime()));
    }

    /**
     * Execute a Datalog query against a DB value.
     */
    QueryResult queryInternal(Db db, String edn) throws IOException {
        return queryInternal(db, edn, List.of());
    }

    /**
     * Execute a Datalog query with input binding arguments.
     */
    QueryResult queryInternal(Db db, String edn, List<QueryArg> args) throws IOException {
        byte[] body = WireCodec.encodeQueryBody(db.txKey(), edn, args);
        byte[] responseBody = postBinary("/db/query", body);
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
                txResult.systemTime(), txResult.errorMessage());
    }

    /**
     * Register an incremental query and stream its result deltas.
     *
     * <p>Subscribes at the latest indexed basis. A non-empty initial result is
     * emitted first at that basis. The returned {@link Subscription} is an
     * {@link AutoCloseable}; closing it cancels the stream and unsubscribes.</p>
     */
    public Subscription subscribe(String edn) throws IOException {
        return subscribe(edn, List.of());
    }

    /**
     * Register an incremental query with {@code :in} bindings.
     */
    public Subscription subscribe(String edn, List<QueryArg> args) throws IOException {
        byte[] body = WireCodec.encodeSubscribeBody(null, edn, args);
        var request = new Request.Builder()
                .url(baseUrl + "/db/subscribe")
                .header("Content-Type", CONTENT_TYPE)
                .post(RequestBody.create(body, CONTENT_MEDIA_TYPE))
                .build();

        Response response = httpClient.newCall(request).execute();

        int status = response.code();
        if (status < 200 || status >= 300) {
            try (response) {
                // Pre-stream error: decode the unary ErrorResponse body.
                byte[] errorBody = response.body() == null ? new byte[0] : response.body().bytes();
                if (errorBody.length > 0) {
                    try {
                        var err = WireCodec.decodeErrorResponse(errorBody);
                        throw new TriploxException(err.severity(), err.code(),
                                err.message(), err.detail(), err.hint());
                    } catch (TriploxException e) {
                        throw e;
                    } catch (Exception ignored) {
                        // Fall through to a generic error.
                    }
                }
                throw new IOException("HTTP error " + status);
            }
        }

        if (response.body() == null) {
            response.close();
            throw new IOException("empty subscription response");
        }

        try {
            return Subscription.open(response.body().byteStream(), response);
        } catch (IOException | RuntimeException e) {
            response.close();
            throw e;
        }
    }

    /**
     * Close the HTTP client.
     */
    @Override
    public void close() {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    // ---------------------------------------------------------------
    // Internal HTTP helpers
    // ---------------------------------------------------------------

    private byte[] postBinary(String path, byte[] body) throws IOException {
        var request = new Request.Builder()
                .url(baseUrl + path)
                .header("Content-Type", CONTENT_TYPE)
                .post(RequestBody.create(body, CONTENT_MEDIA_TYPE))
                .build();
        return sendAndCheck(request);
    }

    private byte[] sendAndCheck(Request request) throws IOException {
        try (Response response = httpClient.newCall(request).execute()) {
            int status = response.code();
            byte[] responseBody = response.body() == null ? new byte[0] : response.body().bytes();
            if (status >= 200 && status < 300) {
                return responseBody;
            }

            // Try to decode binary ErrorResponse from the body
            if (responseBody.length > 0) {
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

}
