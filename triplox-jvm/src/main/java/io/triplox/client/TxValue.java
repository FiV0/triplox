package io.triplox.client;

/**
 * Transaction value variants for the Triplox wire protocol.
 * Data wraps a concrete value; Ref wraps an entity reference.
 */
public sealed interface TxValue {
    record Data(Object value) implements TxValue {}
    record Ref(EntityRef ref) implements TxValue {}
}
