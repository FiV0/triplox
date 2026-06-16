package xyz.triplox.client;

import java.time.Instant;

/**
 * Result of an awaited transaction.
 */
public record TxResult(byte status, long txId, Instant systemTime, String errorMessage) {
    public boolean isCommitted() { return status == 0; }
    public TxKey txKey() { return new TxKey(txId, systemTime); }
}
