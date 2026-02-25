package io.triplox.client;

import java.util.List;

/**
 * Backend (server → client) messages in the Triplox wire protocol.
 */
public sealed interface BackendMessage {
    record AuthenticationOk(String serverVersion) implements BackendMessage {}
    record DbOpened(int dbId, long txId) implements BackendMessage {}
    record DbClosed(int dbId) implements BackendMessage {}
    record RowDescription(List<ColumnDesc> columns) implements BackendMessage {}
    record DataRow(List<Object> values) implements BackendMessage {}
    record DataBatchComplete(long txId) implements BackendMessage {}
    record ReadyForQuery(byte status) implements BackendMessage {}
    record TxKey(long txId, long systemTime) implements BackendMessage {}
    record TxResult(byte status, long txId, long systemTime, long seqNum, String errorMessage) implements BackendMessage {}
    record BasisResult(long txId, long systemTime, long seqNum) implements BackendMessage {}
    record UnsubscribeComplete() implements BackendMessage {}
    record Heartbeat() implements BackendMessage {}
    record ErrorResponse(byte severity, int code, String message, String detail, String hint) implements BackendMessage {}
}
