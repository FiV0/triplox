package io.triplox.client;

import clojure.lang.Keyword;

import java.util.Map;

/**
 * Transaction operations for the Triplox wire protocol.
 */
public sealed interface TxOp {
    record Put(Map<Keyword, TxValue> document) implements TxOp {}
    record Add(EntityRef entityId, Keyword attribute, TxValue value) implements TxOp {}
    record Retract(EntityRef entityId, Keyword attribute, TxValue value) implements TxOp {}
    record Delete(EntityRef entityId) implements TxOp {}
    record Erase(EntityRef entityId) implements TxOp {}
}
