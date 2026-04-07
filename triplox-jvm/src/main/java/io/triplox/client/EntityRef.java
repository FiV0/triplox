package io.triplox.client;

import clojure.lang.Keyword;

/**
 * Entity reference variants for the Triplox wire protocol.
 */
public sealed interface EntityRef {
    record Id(long id) implements EntityRef {}
    record TempId(String tempId) implements EntityRef {}
    record Ident(Keyword ident) implements EntityRef {}
    record LookupRef(Keyword attr, Object value) implements EntityRef {}
}
