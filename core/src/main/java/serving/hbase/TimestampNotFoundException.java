package serving.hbase;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class TimestampNotFoundException extends Exception {
    public TimestampNotFoundException() { super(); }
    public TimestampNotFoundException(String message) { super(message); }
    public TimestampNotFoundException(String message, Throwable cause) { super(message, cause); }
    public TimestampNotFoundException(Throwable cause) { super(cause); }
}
