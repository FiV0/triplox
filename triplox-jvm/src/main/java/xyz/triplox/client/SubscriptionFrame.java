package xyz.triplox.client;

import java.util.List;

/**
 * One frame of a subscription response stream.
 *
 * <p>Frames are bare, self-delimiting msgpack maps tagged by a {@code kind}
 * discriminator. Unrecognized kinds decode to {@link Unknown} so clients can
 * ignore future frame types.</p>
 */
public sealed interface SubscriptionFrame
        permits SubscriptionFrame.Open, Delta, SubscriptionFrame.Error, SubscriptionFrame.Unknown {

    /** First frame: registration basis and (internal) column schema. */
    record Open(TxBasis basis, List<ColumnDesc> columns) implements SubscriptionFrame {}

    /** Terminal error raised after the stream has started. */
    record Error(BackendMessage.ErrorResponse error) implements SubscriptionFrame {}

    /** A frame with an unrecognized {@code kind}; clients ignore it. */
    record Unknown() implements SubscriptionFrame {}
}
