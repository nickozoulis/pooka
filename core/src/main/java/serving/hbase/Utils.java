package serving.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import speed.storm.bolt.Constants;

/**
 * Created by nickozoulis on 13/06/2016.
 */
public class Utils {

    /**
     * Sets HBaseConfiguration according to constants specified in Cons.java
     */
    public static Configuration setHBaseConfig() {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Constants.hbase_IP_address);
        config.set("hbase.zookeeper.property.clientPort", Constants.hbase_port);

        return config;
    }
}
