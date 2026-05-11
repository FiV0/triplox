package io.triplox.client;

import java.time.Instant;

/**
 * Indexed transaction basis for opening an as-of DB snapshot.
 */
public record TxBasis(long txId, Instant systemTime, long txEid) {}
