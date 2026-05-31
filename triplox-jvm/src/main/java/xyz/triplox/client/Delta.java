package xyz.triplox.client;

import java.util.List;

/**
 * One transaction's z-set changes for a subscribed query.
 *
 * <p>{@code basis} may be {@code null} when the engine could not derive a
 * transaction basis; {@code walSeq} is always present and is the fallback
 * ordering key. Each {@link Row} carries the raw signed weight.</p>
 */
public record Delta(TxBasis basis, long walSeq, List<Row> rows) implements SubscriptionFrame {}
