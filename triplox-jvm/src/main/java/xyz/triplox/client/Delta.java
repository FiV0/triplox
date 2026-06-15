package xyz.triplox.client;

import java.util.List;

/**
 * One transaction's z-set changes for a subscribed query.
 *
 * <p>{@code basis} is the transaction basis that produced the delta. Each
 * {@link Row} carries the raw signed weight.</p>
 */
public record Delta(TxKey basis, List<Row> rows) implements SubscriptionFrame {}
