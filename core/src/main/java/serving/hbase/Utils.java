package serving.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import speed.storm.bolt.Cons;

/**
 * Created by nickozoulis on 13/06/2016.
 */
public class Utils {

    /**
     * Sets HBaseConfiguration according to constants specified in Cons.java
     */
    public static Configuration setHBaseConfig() {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Cons.hbase_IP_address);
        config.set("hbase.zookeeper.property.clientPort", Cons.hbase_port);

        return config;
    }

    public static void setHBaseSchema() {

    }

}
