package io.triplox.client;

/**
 * Result of a fire-and-forget transaction (submitTx).
 */
public record TxKeyResult(long txId, long systemTime) {}
