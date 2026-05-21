package xyz.triplox.client;

import java.time.Instant;

/**
 * Transaction key returned after appending a transaction to the log.
 */
public record TxKey(long txId, Instant systemTime) {}
