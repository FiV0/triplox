package io.triplox.client;

/**
 * Describes a single column in a query result.
 */
public record ColumnDesc(String name, byte dataType) {}
