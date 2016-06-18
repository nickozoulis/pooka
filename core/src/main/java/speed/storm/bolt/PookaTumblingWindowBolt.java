package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;
import serving.hbase.Utils;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;


/**
 * Created by nickozoulis on 12/06/2016.
 */
public abstract class PookaTumblingWindowBolt extends BaseWindowedBolt implements Serializable {
    private static final long serialVersionUID = -2268993895150431399L;
    private OutputCollector collector;
    private HTable tableSpeed, tableRaw;
    private static boolean AUTO_FLUSH = false;
    private static boolean CLEAR_BUFFER_ON_FAIL = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;

        try {
            Configuration conf = Utils.setHBaseConfig();

            tableSpeed = new HTable(conf, Cons.TABLE_SPEED);
            tableRaw = new HTable(conf, Cons.MASTER_DATASET);

            tableSpeed.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
            tableRaw.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public abstract void execute(TupleWindow inputWindow);

    public abstract void writeSpeedViewToHBase() throws IOException;

    public abstract void writeRawToHBase() throws IOException;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        try {
            tableSpeed.close();
            tableRaw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected OutputCollector getCollector() {
        return this.collector;
    }

    protected HTable getTableSpeed() {
        return this.tableSpeed;
    }

    protected HTable getTableRaw() {
        return this.tableRaw;
    }
 }
