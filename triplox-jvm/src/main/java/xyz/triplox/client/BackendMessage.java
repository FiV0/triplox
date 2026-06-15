package xyz.triplox.client;

import java.time.Instant;

/**
 * Decoded HTTP response bodies. Used internally by {@link WireCodec}.
 */
public sealed interface BackendMessage {
    record TxKey(long txId, Instant systemTime) implements BackendMessage {}
    record TxResult(byte status, long txId, Instant systemTime, String errorMessage)
            implements BackendMessage {}
    record ErrorResponse(byte severity, short code, String message, String detail, String hint)
            implements BackendMessage {}
}
