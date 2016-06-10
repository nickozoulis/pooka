package source.kafka;

import java.util.Iterator;

/**
 * Created by nickozoulis on 09/06/2016.
 */
interface Consumer extends Iterator {
    void open();
    Iterator fetch();
    void close();
}
