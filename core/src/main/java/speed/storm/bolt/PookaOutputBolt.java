package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import serving.hbase.Utils;
import speed.storm.PookaView;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public abstract class PookaOutputBolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 5897481848220051413L;
    private Map<Long, PookaView> views;
    private Map<Long, List<Put>> rawPuts;
    private OutputCollector collector;
    private HTable tableSpeed, tableRaw;
    private static boolean AUTO_FLUSH = false;
    private static boolean CLEAR_BUFFER_ON_FAIL = false;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        try {
            Configuration conf = Utils.setHBaseConfig();

            tableSpeed = new HTable(conf, Cons.TABLE_SPEED);
            tableRaw = new HTable(conf, Cons.MASTER_DATASET);

            tableSpeed.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
            tableRaw.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);

            setViews(new HashMap<Long, PookaView>());
            setRawPuts(new HashMap<Long, List<Put>>());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public abstract void execute(Tuple input);

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

    public Map<Long, PookaView> getViews() {
        return views;
    }

    public void setViews(Map<Long, PookaView> views) {
        this.views = views;
    }

    public OutputCollector getCollector() {
        return collector;
    }

    public void setCollector(OutputCollector collector) {
        this.collector = collector;
    }

    protected HTable getTableSpeed() {
        return this.tableSpeed;
    }

    protected HTable getTableRaw() {
        return this.tableRaw;
    }

    public Map<Long, List<Put>> getRawPuts() {
        return rawPuts;
    }

    public void setRawPuts(Map<Long, List<Put>> rawPuts) {
        this.rawPuts = rawPuts;
    }
}
