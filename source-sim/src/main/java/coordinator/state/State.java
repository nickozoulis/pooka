package coordinator.state;

import org.apache.hadoop.hbase.client.Result;

import java.util.Map;

/**
 * Created by nickozoulis on 11/07/2016.
 */
public interface State<K, V> {
    void process(Result result);
    Map<K, V> getState();
}
