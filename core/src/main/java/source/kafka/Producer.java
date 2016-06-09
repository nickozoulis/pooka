package source.kafka;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public interface Producer {
    void open();
    void send();
    void close();
}
