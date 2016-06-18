package source.kafka;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public interface IProducer<K, V> {
    void open();
    void send(K k, V v);
    void send(V v);
    void close();
}
