package serving.query_handlers;

/**
 * Created by nickozoulis on 19/07/2016.
 */
public class QueryNotSupportedException extends Exception {
    public QueryNotSupportedException() { super(); }
    public QueryNotSupportedException(String message) { super(message); }
    public QueryNotSupportedException(String message, Throwable cause) { super(message, cause); }
    public QueryNotSupportedException(Throwable cause) { super(cause); }
}

