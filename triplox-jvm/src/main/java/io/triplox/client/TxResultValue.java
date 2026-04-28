package io.triplox.client;

import java.time.Instant;

/**
 * Result of an awaited transaction.
 */
public record TxResultValue(byte status, long txId, Instant systemTime, String errorMessage) {
    public boolean isCommitted() { return status == 0; }
}
