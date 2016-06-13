package speed.storm.bolt;

/**
 * Created by nickozoulis on 12/06/2016.
 */
public class Constants {
    static final String TABLE_SPEED = "speed_views";
    static final String MASTER_DATASET = "master_dataset";
    static final byte[] COLUMN_FAMILY_SPEED = "v".getBytes();
    static final byte[] COLUMN_FAMILY_MASTER_DATASET = "m".getBytes();
    static final byte[] COLUMN_COLOR = "COLOR".getBytes();
    static final byte[] COLUMN_COUNT = "COUNT".getBytes();
    public static final String hbase_IP_address = "127.0.0.1";
    public static final String hbase_port = "2181";
}
