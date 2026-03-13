package io.triplox.client;

/**
 * Exception wrapping a Triplox ErrorResponse.
 */
public class TriploxException extends RuntimeException {
    private final byte severity;
    private final short code;
    private final String detail;
    private final String hint;

    public TriploxException(byte severity, short code, String message, String detail, String hint) {
        super(message);
        this.severity = severity;
        this.code = code;
        this.detail = detail;
        this.hint = hint;
    }

    public byte severity() { return severity; }
    public short code() { return code; }
    public String detail() { return detail; }
    public String hint() { return hint; }

    public boolean isFatal() {
        return severity == MessageTypes.SEVERITY_FATAL;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        sb.append(isFatal() ? "FATAL" : "ERROR");
        sb.append(" [").append(Short.toUnsignedInt(code)).append("]: ").append(getMessage());
        if (detail != null) sb.append("\nDetail: ").append(detail);
        if (hint != null) sb.append("\nHint: ").append(hint);
        return sb.toString();
    }
}
