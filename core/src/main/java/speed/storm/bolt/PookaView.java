package speed.storm.bolt;

import java.util.Map;

/**
 * Created by nickozoulis on 22/06/2016.
 */
public interface PookaView<K, V> {
    Map<K, V> getView();
}