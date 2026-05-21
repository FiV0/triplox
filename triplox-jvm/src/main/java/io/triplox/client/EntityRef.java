package io.triplox.client;

/**
 * Entity reference variants for the Triplox wire protocol.
 */
public sealed interface EntityRef {
    record Id(long id) implements EntityRef {}
    record TempId(String tempId) implements EntityRef {}
    record Ident(String ident) implements EntityRef {}
    record LookupRef(String attr, Object value) implements EntityRef {}
}
