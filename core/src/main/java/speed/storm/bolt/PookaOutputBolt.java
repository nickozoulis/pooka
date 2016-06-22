package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import serving.hbase.Utils;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public abstract class PookaOutputBolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 93955065037014054L;
    private OutputCollector collector;
    private final int numOfInputBolts;
    private PookaBundle pookaBundle;
    private transient HTable tableSpeed, tableRaw;
    private boolean AUTO_FLUSH = false;
    private boolean CLEAR_BUFFER_ON_FAIL = false;
    // For monitoring purpose
    protected int TASK_ID;

    public PookaOutputBolt(int numOfInputBolts) {
        this.numOfInputBolts = numOfInputBolts;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        TASK_ID = context.getThisTaskId();
        try {
            Configuration conf = Utils.setHBaseConfig();

            tableSpeed = new HTable(conf, Cons.TABLE_SPEED);
            tableRaw = new HTable(conf, Cons.MASTER_DATASET);

            tableSpeed.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
            tableRaw.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
        }

        setPookaBundle(new PookaBundle(numOfInputBolts));
    }

    @Override
    public abstract void execute(Tuple input);

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public void cleanup() {
        try {
            tableSpeed.close();
            tableRaw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getNumOfInputBolts() {
        return numOfInputBolts;
    }

    public PookaBundle getPookaBundle() {
        return pookaBundle;
    }

    public void setPookaBundle(PookaBundle pookaBundle) {
        this.pookaBundle = pookaBundle;
    }

    protected byte[] toBytes(String b) {
        return Bytes.toBytes(b);
    }

    protected byte[] toBytes(Integer b) {
        return Bytes.toBytes(b);
    }

    protected byte[] toBytes(Double b) {
        return Bytes.toBytes(b);
    }

    public HTable getTableRaw() {
        return this.tableRaw;
    }

    public HTable getTableSpeed() {
        return this.tableSpeed;
    }
}
