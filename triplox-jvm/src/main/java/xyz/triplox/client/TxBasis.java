package xyz.triplox.client;

import java.time.Instant;

/**
 * Indexed transaction basis for opening an as-of DB read handle.
 */
public record TxBasis(long txId, Instant systemTime, long txEid) {}
