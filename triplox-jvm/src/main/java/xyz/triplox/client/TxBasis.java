package xyz.triplox.client;

import java.time.Instant;

/**
 * Indexed transaction basis for an as-of DB value.
 */
public record TxBasis(long txId, Instant systemTime, long txEid) {}
