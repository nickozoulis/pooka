package source.kafka;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public interface Consumer extends Iterator<Map<?,?>> {
    void open();
    void close();
}
