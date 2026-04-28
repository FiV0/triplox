package io.triplox.client;

import java.time.Instant;

/**
 * Result of a fire-and-forget transaction.
 */
public record TxKeyResult(long txId, Instant systemTime) {}
