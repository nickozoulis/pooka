package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * Created by nickozoulis on 12/06/2016.
 */
//TODO: Adjust to my use case
public class HBaseInsertBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Long lastWindowId;
    private HConnection connection;
    private static boolean AUTO_FLUSH = false;
    private static boolean CLEAR_BUFFER_ON_FAIL = false;
    private HTableInterface table;
    private Put p;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        lastWindowId = Long.MIN_VALUE;
        Configuration config = HBaseConfiguration.create();
        try {
            connection = HConnectionManager.createConnection(config);
            //FIXME: Take table name from stormConf
            table = connection.getTable(Constants.TABLE_SPEED);
            table.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        Integer count = input.getInteger(1);
        Long windowId = input.getLong(2);

        try {
            if (windowId != lastWindowId) {// add columns in the same row
                p = new Put(Bytes.toBytes(windowId), windowId);
            }
            
            p.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(word), Bytes.toBytes(count));

            table.put(p);
        } catch (Exception e) {
//            LOG.error("bolt error", e);
            collector.reportError(e);
        }
        lastWindowId = windowId;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
//        LOG.info("cleanup called");
        try {
            table.close();
            connection.close();
//            LOG.info("hbase closed");
        } catch (Exception e) {
//            LOG.error("cleanup error", e);
        }
    }

}
