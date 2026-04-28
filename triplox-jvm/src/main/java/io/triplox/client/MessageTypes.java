package io.triplox.client;

/**
 * Protocol constants for the Triplox wire protocol (version 0.1).
 */
public final class MessageTypes {
    private MessageTypes() {}

    // Protocol version
    public static final short PROTOCOL_VERSION_MAJOR = 0;
    public static final short PROTOCOL_VERSION_MINOR = 1;

    // Frontend message type bytes (client → server)
    public static final byte MSG_OPEN_DB      = (byte) 'O';
    public static final byte MSG_CLOSE_DB     = (byte) 'L';
    public static final byte MSG_QUERY        = (byte) 'Q';
    public static final byte MSG_EXECUTE      = (byte) 'E';
    public static final byte MSG_SUBSCRIBE    = (byte) 'S';
    public static final byte MSG_UNSUBSCRIBE  = (byte) 'U';
    public static final byte MSG_TERMINATE    = (byte) 'X';

    // Backend message type bytes (server → client)
    public static final byte MSG_AUTHENTICATION_OK    = (byte) 'R';
    public static final byte MSG_DB_OPENED            = (byte) 'H';
    public static final byte MSG_DB_CLOSED            = (byte) 'J';
    public static final byte MSG_ROW_DESCRIPTION      = (byte) 'T';
    public static final byte MSG_DATA_ROW             = (byte) 'D';
    public static final byte MSG_DATA_BATCH_COMPLETE   = (byte) 'B';
    public static final byte MSG_READY_FOR_QUERY       = (byte) 'Z';
    public static final byte MSG_TX_KEY                = (byte) 'Y';
    public static final byte MSG_TX_RESULT             = (byte) 'G';
    public static final byte MSG_UNSUBSCRIBE_COMPLETE  = (byte) 'N';
    public static final byte MSG_HEARTBEAT             = (byte) 'K';
    public static final byte MSG_ERROR_RESPONSE        = (byte) 'W';

    // ReadyForQuery status bytes
    public static final byte STATUS_IDLE       = (byte) 'I';
    public static final byte STATUS_SUBSCRIBED = (byte) 'S';

    // ErrorResponse severity bytes
    public static final byte SEVERITY_ERROR = (byte) 'E';
    public static final byte SEVERITY_FATAL = (byte) 'F';

    // DataType tag bytes
    public static final byte TAG_BIG_INT  = 1;
    public static final byte TAG_BOOLEAN  = 2;
    public static final byte TAG_BYTES    = 3;
    public static final byte TAG_DOUBLE   = 4;
    public static final byte TAG_FLOAT    = 5;
    public static final byte TAG_INSTANT  = 6;
    public static final byte TAG_LONG     = 7;
    public static final byte TAG_REF      = 8;
    public static final byte TAG_STRING   = 9;
    public static final byte TAG_UUID     = 10;
    public static final byte TAG_VECTOR   = 11;
    public static final byte TAG_MAP      = 12;
    public static final byte TAG_KEYWORD  = 13;
    public static final byte TAG_UNKNOWN  = (byte) 255;

    // TxOp tag bytes
    public static final byte TXOP_PUT     = 0;
    public static final byte TXOP_ADD     = 1;
    public static final byte TXOP_RETRACT = 2;
    public static final byte TXOP_DELETE  = 3;
    public static final byte TXOP_ERASE   = 4;

    // EntityRef tag bytes
    public static final byte ENTITY_REF_ID      = (byte) 0x90;
    public static final byte ENTITY_REF_TEMPID  = (byte) 0x91;
    public static final byte ENTITY_REF_IDENT   = (byte) 0x92;
    public static final byte ENTITY_REF_LOOKUP  = (byte) 0x93;

    // QueryArg tag bytes
    public static final byte QUERY_ARG_SCALAR     = 0;
    public static final byte QUERY_ARG_COLLECTION = 1;
    public static final byte QUERY_ARG_TUPLE      = 2;
    public static final byte QUERY_ARG_RELATION   = 3;
}
