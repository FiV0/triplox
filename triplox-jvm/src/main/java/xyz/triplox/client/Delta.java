package xyz.triplox.client;

import java.util.List;

/**
 * A subscribed query's weighted result changes.
 *
 * <p>{@code txKey} is the registration basis for a priming delta, or the
 * transaction key that produced a later delta. Each {@link Row} carries the raw signed weight.</p>
 */
public record Delta(TxKey txKey, List<Row> rows) implements SubscriptionFrame {}
