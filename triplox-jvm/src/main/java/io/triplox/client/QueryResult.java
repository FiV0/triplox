package io.triplox.client;

import java.util.List;

/**
 * Result of a query: column descriptions + rows of values.
 */
public record QueryResult(List<ColumnDesc> columns, List<List<Object>> rows) {}
