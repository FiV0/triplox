package xyz.triplox.client;

import java.util.List;

/**
 * One frame of a subscription response stream.
 *
 * <p>Frames are bare, self-delimiting msgpack maps tagged by a {@code kind}
 * discriminator.</p>
 */
public sealed interface SubscriptionFrame
        permits SubscriptionFrame.Open, Delta, SubscriptionFrame.Error {

    /** First frame: registration tx_key and (internal) column schema. */
    record Open(TxKey txKey, List<ColumnDesc> columns) implements SubscriptionFrame {}

    /** Terminal error raised after the stream has started. */
    record Error(BackendMessage.ErrorResponse error) implements SubscriptionFrame {}
}
