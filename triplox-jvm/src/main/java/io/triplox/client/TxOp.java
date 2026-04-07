package io.triplox.client;

import clojure.lang.Keyword;

import java.util.Map;

/**
 * Transaction operations for the Triplox wire protocol.
 */
public sealed interface TxOp {
    record Put(Map<Keyword, Object> document) implements TxOp {}
    record Add(EntityRef entity, Keyword attribute, Object value) implements TxOp {}
    record Retract(EntityRef entity, Keyword attribute, Object value) implements TxOp {}
    record Delete(EntityRef entity) implements TxOp {}
    record Erase(EntityRef entity) implements TxOp {}
}
