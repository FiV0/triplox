package io.triplox.client;

/**
 * Handle to an open DB snapshot on the server.
 */
public record DbHandle(int dbId, long txId) {}
