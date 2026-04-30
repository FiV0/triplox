package io.triplox.client;

/**
 * DataType column tags and ErrorResponse severity bytes.
 *
 * <p>Tag values match {@code src/protocol.rs} and are used as the
 * {@code data_type} field of a {@link ColumnDesc}. Concrete column types are
 * 1..13; {@link #TAG_UNION} (127) and {@link #TAG_UNKNOWN} (255) are
 * column-description-only tags.</p>
 */
public final class MessageTypes {
    private MessageTypes() {}

    // DataType column type tags
    public static final byte TAG_BIG_INT  = 1;
    public static final byte TAG_BOOLEAN  = 2;
    public static final byte TAG_BYTES    = 3;
    public static final byte TAG_DOUBLE   = 4;
    public static final byte TAG_FLOAT    = 5;
    public static final byte TAG_INSTANT  = 6;
    public static final byte TAG_LONG     = 7;
    /** Reserved for a future {@code DataType::Ref} variant. */
    public static final byte TAG_REF      = 8;
    public static final byte TAG_STRING   = 9;
    public static final byte TAG_UUID     = 10;
    public static final byte TAG_VECTOR   = 11;
    public static final byte TAG_MAP      = 12;
    public static final byte TAG_KEYWORD  = 13;
    /** Column-description-only tag for unions of concrete types. */
    public static final byte TAG_UNION    = 127;
    /** Column-description-only tag for indeterminate column types. */
    public static final byte TAG_UNKNOWN  = (byte) 255;

    // ErrorResponse severity bytes
    public static final byte SEVERITY_ERROR = (byte) 'E';
    public static final byte SEVERITY_FATAL = (byte) 'F';

    // MessagePack ext type codes (must match crate::msgpack_codec)
    public static final byte EXT_TIMESTAMP = -1;
    public static final byte EXT_BIGINT    = 1;
    public static final byte EXT_UUID      = 2;
    public static final byte EXT_KEYWORD   = 3;
}
