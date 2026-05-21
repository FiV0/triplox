package xyz.triplox.client;

import java.util.List;

/**
 * Describes a single column in a query result.
 *
 * <p>For concrete column types and {@link MessageTypes#TAG_UNKNOWN}, {@code members}
 * is {@code null}. For {@link MessageTypes#TAG_UNION} columns it carries the list of
 * concrete member tags.</p>
 */
public record ColumnDesc(String name, byte dataType, List<Byte> members) {
    public ColumnDesc(String name, byte dataType) {
        this(name, dataType, null);
    }
}
