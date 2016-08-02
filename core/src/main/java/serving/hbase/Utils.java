package serving.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import speed.storm.bolt.Cons;

import java.io.IOException;

/**
 * Created by nickozoulis on 13/06/2016.
 */
public class Utils {

    private static final Logger logger = Logger.getLogger(Utils.class);

    /**
     * Sets HBaseConfiguration according to constants specified in Cons.java
     */
    public static Configuration setHBaseConfig() {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", Cons.localhost);
        config.set("hbase.zookeeper.property.clientPort", Cons.zk_port);
        // SocketTimeoutException issue from HBase Client
        config.set("hbase.rpc.timeout", "1800000");

        return config;
    }

    public static void deleteAllSchemaTables() {
        try {
            // Instantiating HbaseAdmin class
            HBaseAdmin admin = new HBaseAdmin(setHBaseConfig());

            if (admin.tableExists(Cons.MASTER_DATASET)) {
                admin.disableTable(Cons.MASTER_DATASET);
                admin.deleteTable(Cons.MASTER_DATASET);
                logger.info("Table deleted: " + Cons.MASTER_DATASET);
            }

            if (admin.tableExists(Cons.TABLE_BATCH)) {
                admin.disableTable(Cons.TABLE_BATCH);
                admin.deleteTable(Cons.TABLE_BATCH);
                logger.info("Table deleted: " + Cons.TABLE_BATCH);
            }

            if (admin.tableExists(Cons.TABLE_SPEED)) {
                admin.disableTable(Cons.TABLE_SPEED);
                admin.deleteTable(Cons.TABLE_SPEED);
                logger.info("Table deleted: " + Cons.TABLE_SPEED);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createAllSchemaTables() {
        try {
            HBaseAdmin admin = new HBaseAdmin(setHBaseConfig());

            // Master Dataset
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.MASTER_DATASET));
            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.CF_MASTER_DATASET_INFO));
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.CF_MASTER_DATASET_OTHER));

            if (!admin.tableExists(Cons.MASTER_DATASET)) {
                admin.createTable(tableDescriptor);
                logger.info("Table created: " + tableDescriptor.getNameAsString());
            }

            // Batch Views
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.TABLE_BATCH));
            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.CF_VIEWS));

            if (!admin.tableExists(Cons.TABLE_BATCH)) {
                admin.createTable(tableDescriptor);
                logger.info("Table created: " + tableDescriptor.getNameAsString());
            }

            // Master Dataset
            tableDescriptor = new HTableDescriptor(TableName.valueOf(Cons.TABLE_SPEED));
            // Adding column families to table descriptor
            tableDescriptor.addFamily(new HColumnDescriptor(Cons.CF_VIEWS));

            if (!admin.tableExists(Cons.TABLE_SPEED)) {
                admin.createTable(tableDescriptor);
                logger.info("Table created: " + tableDescriptor.getNameAsString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
