package io.triplox.client;

/**
 * Result of an awaited transaction.
 */
public record TxResultValue(byte status, long txId, long systemTime, String errorMessage) {
    public boolean isCommitted() { return status == 0; }
}
