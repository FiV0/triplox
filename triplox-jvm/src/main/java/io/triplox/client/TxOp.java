package io.triplox.client;

import java.util.Map;

/**
 * Transaction operations for the Triplox wire protocol.
 */
public sealed interface TxOp {
    record Put(Map<String, Object> document) implements TxOp {}
    record Add(long entity, String attribute, Object value) implements TxOp {}
    record Retract(long entity, String attribute, Object value) implements TxOp {}
    record Delete(long entity) implements TxOp {}
    record Erase(long entity) implements TxOp {}
}
