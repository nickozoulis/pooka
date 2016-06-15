package speed.storm.bolt;

/**
 * Created by nickozoulis on 12/06/2016.
 */
public class Cons {
    public static final String TABLE_SPEED = "speed_views";
    public static final String MASTER_DATASET = "master_dataset";
    public static final String COLUMN_FAMILY_SPEED = "v";
    public static final String COLUMN_FAMILY_MASTER_DATASET = "m";
    public static final byte[] COLUMN_COLOR = "COLOR".getBytes();
    public static final byte[] COLUMN_COUNT = "COUNT".getBytes();
    public static final String hbase_IP_address = "127.0.0.1";
    public static final String hbase_port = "2181";
    public static final boolean AUTO_FLUSH = false;
    public static final boolean CLEAR_BUFFER_ON_FAIL = false;
}
