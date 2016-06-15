package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import serving.hbase.Utils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by nickozoulis on 12/06/2016.
 */
public class TumblingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private Map<String, Integer> counters;
    private Long windowId;
    private Set<String> rawData;
    private HConnection connection;
    private HTable tableSpeed, tableRaw;
    private static boolean AUTO_FLUSH = false;
    private static boolean CLEAR_BUFFER_ON_FAIL = false;
    private Put p;

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
    public void execute(TupleWindow inputWindow) {
        rawData = new HashSet<>();
        counters = new HashMap<>();
        windowId = System.currentTimeMillis();

        for (Tuple tuple : inputWindow.get()) {
            rawData.add(tuple.getString(0));
            String str = tuple.getString(1);

            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            collector.ack(tuple);
        }

        writeToHBase(rawData, counters);
    }

    private void writeToHBase(Set<String> s, Map<String, Integer> m) {
        try {
            writeRawToHBase(s);
            writeSpeedViewToHBase(m);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeSpeedViewToHBase(Map<String, Integer> m) throws IOException {
        p = new Put(Bytes.toBytes(windowId), windowId);
        for (Map.Entry<String, Integer> entry : m.entrySet()) {
            System.out.println(">>>>>>>>> "+entry.getKey() + " " + entry.getValue());
            p.addColumn(Cons.COLUMN_FAMILY_SPEED.getBytes(), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        tableSpeed.put(p);
    }

    private void writeRawToHBase(Set<String> s) throws IOException {
        for (String str : s) {
            System.out.println(">>>>>>>>> "+str);
            p = new Put(Bytes.toBytes(str), windowId);
            p.addColumn(Cons.COLUMN_FAMILY_MASTER_DATASET.getBytes(),
                    Bytes.toBytes(""), Bytes.toBytes(""));
            tableRaw.put(p);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
//        LOG.info("cleanup called");
        try {
            tableSpeed.close();
            tableRaw.close();
            connection.close();
//            LOG.info("hbase closed");
        } catch (Exception e) {
//            LOG.error("cleanup error", e);
        }
    }
}
