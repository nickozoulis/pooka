package source.kafka;

import java.util.Map;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public interface Producer<K, V> {
    void open();
    void send(Map<K, V> map);
    void close();
}
