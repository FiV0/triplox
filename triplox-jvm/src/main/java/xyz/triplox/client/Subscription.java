package xyz.triplox.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;

/**
 * A live incremental query subscription over a streaming HTTP/2 response.
 *
 * <p>{@link #take()} blocks for the next {@link Delta}; {@link #poll} bounds the
 * wait. A background daemon thread decodes frames into a bounded queue, so a slow
 * consumer applies backpressure — the reader stops draining the socket when the
 * queue is full, which the server observes via HTTP/2 flow control. A terminal
 * {@code error} frame surfaces as a {@link TriploxException} from the next
 * {@code take}/{@code poll}. {@link #close()} cancels the stream and unsubscribes.</p>
 *
 * <p>Thread-safety: consume a single subscription from one thread.</p>
 */
public final class Subscription implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 128;
    private static final Object END = new Object();

    private final TxBasis basis;
    private final InputStream stream;
    private final Closeable closeable;
    private final Thread reader;
    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    private volatile boolean closed = false;
    private volatile boolean drained = false;
    private volatile TriploxException terminalError;

    private Subscription(
            TxBasis basis, InputStream stream, Closeable closeable, MessageUnpacker unpacker) {
        this.basis = basis;
        this.stream = stream;
        this.closeable = closeable;
        this.reader = new Thread(() -> readLoop(unpacker), "triplox-subscription-reader");
        this.reader.setDaemon(true);
        this.reader.start();
    }

    /**
     * Wrap a streaming subscription response: read the leading {@code open} frame
     * for {@link #basis()}, then start the reader thread.
     */
    static Subscription open(InputStream stream) throws IOException {
        return open(stream, stream);
    }

    static Subscription open(InputStream stream, Closeable closeable) throws IOException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(stream);
        SubscriptionFrame first = WireCodec.decodeSubscriptionFrame(unpacker);
        if (first instanceof SubscriptionFrame.Open open) {
            return new Subscription(open.basis(), stream, closeable, unpacker);
        }
        closeable.close();
        if (first instanceof SubscriptionFrame.Error error) {
            throw toException(error.error());
        }
        throw new IOException("expected open frame, got " + first);
    }

    /** The registration basis. Deltas describe transactions strictly after it. */
    public TxBasis basis() {
        return basis;
    }

    /** True once the stream has ended and the consumer has observed the end. */
    public boolean isDone() {
        return drained;
    }

    /** Block for the next delta. Returns {@code null} when the stream ends. */
    public Delta take() throws InterruptedException {
        if (drained) {
            return endResult();
        }
        return unwrap(queue.take());
    }

    /** Wait up to {@code timeout} for the next delta; {@code null} on timeout or end. */
    public Delta poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (drained) {
            return endResult();
        }
        Object item = queue.poll(timeout, unit);
        return item == null ? null : unwrap(item);
    }

    @Override
    public void close() {
        closed = true;
        try {
            closeable.close();
        } catch (IOException ignored) {
            // Closing to unblock the reader; errors here are expected.
        }
        reader.interrupt();
    }

    private Delta unwrap(Object item) {
        if (item == END) {
            drained = true;
            return endResult();
        }
        return (Delta) item;
    }

    private Delta endResult() {
        if (terminalError != null) {
            throw terminalError;
        }
        return null;
    }

    private void readLoop(MessageUnpacker unpacker) {
        try {
            while (!closed) {
                if (!unpacker.hasNext()) {
                    break;
                }
                SubscriptionFrame frame = WireCodec.decodeSubscriptionFrame(unpacker);
                if (frame instanceof Delta delta) {
                    queue.put(delta);
                } else if (frame instanceof SubscriptionFrame.Error error) {
                    terminalError = toException(error.error());
                    break;
                }
                // Open (unexpected mid-stream) and Unknown frames are ignored.
            }
        } catch (IOException e) {
            // Stream closed or ended; signal end below.
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                closeable.close();
            } catch (IOException ignored) {
                // The consumer may have already closed the subscription.
            }
            try {
                queue.put(END);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static TriploxException toException(BackendMessage.ErrorResponse e) {
        return new TriploxException(e.severity(), e.code(), e.message(), e.detail(), e.hint());
    }
}
