package xyz.triplox.client;

import java.util.Map;

/**
 * Transaction operations for the Triplox wire protocol.
 */
public sealed interface TxOp {
    record Put(Map<String, Object> document) implements TxOp {}
    record Add(EntityRef entity, String attribute, Object value) implements TxOp {}
    record Retract(EntityRef entity, String attribute, Object value) implements TxOp {}
    record RetractEntity(EntityRef entity) implements TxOp {}
    record Erase(EntityRef entity) implements TxOp {}
}
