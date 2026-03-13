package io.triplox.client;

/**
 * Result of a fire-and-forget transaction.
 */
public record TxKeyResult(long txId, long systemTime) {}
