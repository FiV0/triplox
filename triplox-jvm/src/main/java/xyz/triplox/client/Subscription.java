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
 * {@code error} frame or invalid stream state surfaces from the next {@code
 * take}/{@code poll}. {@link #close()} cancels the stream and unsubscribes.</p>
 *
 * <p>Thread-safety: consume a single subscription from one thread.</p>
 */
public final class Subscription implements AutoCloseable {
    private static final int QUEUE_CAPACITY = 128;
    private static final short INTERNAL_ERROR = 4000;

    private final TxKey txKey;
    private final Closeable closeable;
    private final Thread reader;
    private final BlockingQueue<QueueEvent> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

    private volatile boolean closed = false;
    private volatile RuntimeException terminalError;

    // The main reason we have this end sentinel is to unblock takes/polls when the stream ends or the subscription is closed
    private sealed interface QueueEvent permits QueueEvent.DeltaEvent, QueueEvent.End {
        record DeltaEvent(Delta delta) implements QueueEvent {}

        enum End implements QueueEvent {
            INSTANCE
        }
    }

    Subscription(TxKey txKey, Closeable closeable, MessageUnpacker unpacker) {
        this.txKey = txKey;
        this.closeable = closeable;
        this.reader = new Thread(() -> readLoop(unpacker), "triplox-subscription-reader");
        this.reader.setDaemon(true);
        this.reader.start();
    }

    /**
     * Wrap a streaming subscription response: read the leading {@code open} frame
     * for {@link #txKey()}, then start the reader thread.
     */
    static Subscription open(InputStream stream) throws IOException {
        return open(stream, stream);
    }

    static Subscription open(InputStream stream, Closeable closeable) throws IOException, TriploxException {
        MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(stream);
        SubscriptionFrame first = WireCodec.decodeSubscriptionFrame(unpacker);
        if (first instanceof SubscriptionFrame.Open open) {
            return new Subscription(open.txKey(), closeable, unpacker);
        }
        closeable.close();
        if (first instanceof SubscriptionFrame.Error(BackendMessage.ErrorResponse error1)) {
            throw toException(error1);
        }
        throw new IOException("expected open frame, got " + first);
    }

    /** The registration tx_key. A priming delta can equal it; later deltas are strictly after it. */
    public TxKey txKey() {
        return txKey;
    }

    /** True once the stream has ended or the subscription has been closed. */
    public boolean isDone() {
        return closed;
    }

    private Delta endResult() {
        if (terminalError != null) {
            throw terminalError;
        }
        return null;
    }

    private Delta unwrap(QueueEvent event) {
        if (event instanceof QueueEvent.DeltaEvent(Delta delta1)) {
            return delta1;
        }
        closed = true;
        return endResult();
    }

    /** Block for the next delta. Returns {@code null} when the stream ends. */
    public Delta take() throws InterruptedException {
        if (closed) {
            return endResult();
        }
        return unwrap(queue.take());
    }

    /** Wait up to {@code timeout} for the next delta; {@code null} on timeout or end. */
    public Delta poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return endResult();
        }
        QueueEvent event = queue.poll(timeout, unit);
        return event == null ? null : unwrap(event);
    }

    @Override
    public void close() {
        closed = true;
        queue.clear();
        queue.offer(QueueEvent.End.INSTANCE);
        try {
            closeable.close();
        } catch (IOException ignored) {
            // Closing to unblock the reader; errors here are expected.
        }
        reader.interrupt();
    }


    private void readLoop(MessageUnpacker unpacker) {
        try {
            while (!closed) {
                if (!unpacker.hasNext()) {
                    queue.put(QueueEvent.End.INSTANCE);
                    break;
                }
                SubscriptionFrame frame = WireCodec.decodeSubscriptionFrame(unpacker);
                switch (frame) {
                    case Delta delta -> queue.put(new QueueEvent.DeltaEvent(delta));
                    case SubscriptionFrame.Error(BackendMessage.ErrorResponse error1) -> {
                        finishWithError(toException(error1));
                        return;
                    }
                    case SubscriptionFrame.Open ignored -> {
                        finishWithError(new IllegalStateException("unexpected open frame mid-stream"));
                        return;
                    }
                }
            }
        } catch (IOException e) {
            if (!closed) {
                finishWithError(new TriploxException(
                        MessageTypes.SEVERITY_ERROR,
                        INTERNAL_ERROR,
                        "subscription stream failed: " + e.getMessage(),
                        null,
                        null,
                        e));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            try {
                closeable.close();
            } catch (IOException ignored) {
                // The consumer may have already closed the subscription.
            }
        }
    }

    private void finishWithError(RuntimeException error) {
        terminalError = error;
        try {
            queue.put(QueueEvent.End.INSTANCE);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static TriploxException toException(BackendMessage.ErrorResponse e) {
        return new TriploxException(e.severity(), e.code(), e.message(), e.detail(), e.hint());
    }

}
