package xyz.triplox.client;

import java.time.Instant;

/**
 * Result of an awaited transaction.
 */
public record TxResult(byte status, long txId, Instant systemTime, long txEid, String errorMessage) {
    public boolean isCommitted() { return status == 0; }
    public TxBasis basis() { return new TxBasis(txId, systemTime, txEid); }
}
