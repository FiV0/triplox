package io.triplox.client;

/**
 * Result of an awaited transaction (executeTx).
 */
public record TxResultValue(byte status, long txId, long systemTime, long seqNum, String errorMessage) {
    public boolean isCommitted() { return status == 0; }
}
