package speed.storm.bolt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.xalan.xsltc.util.IntegerArray;
import serving.hbase.Utils;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public abstract class PookaOutputBolt extends BaseRichBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(PookaOutputBolt.class);
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

    protected void flush(Long window, String queryPrefix) {
        logger.info("Flushing task: " + TASK_ID + ", window: " + window);
        try {
            // Write raw data to master dataset in HBase.
            List<Put> p = getPookaBundle().getRawPuts().get(window);
            logger.info("The size of raw tuples to be flushed to master dataset is : " + p.size());
            getTableRaw().put(p);
            logger.info("Flushed raw tuples to HBase");
            // Write speed views to speed view table in HBase.
            getTableSpeed().put(createPutFromView(window, queryPrefix));
            logger.info("Flushed speed views to HBase");
            // Remove data from bundle to release memory
            getPookaBundle().removeFromBundle(window);
            logger.info("Removed PookaView and auxiliary data from memory");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Put createPutFromView(Long window, String queryPrefix) throws IOException {
        Put p = new Put(Bytes.toBytes(window), window);

        Map m = getPookaBundle().getViewMap().get(window).getView();
        Iterator it = m.entrySet().iterator();
        Map.Entry pair;

        while (it.hasNext()) {
            pair = (Map.Entry)it.next();
            p.addColumn(Cons.CF_VIEWS.getBytes(), toBytes(queryPrefix + pair.getKey()), toBytes(pair.getValue()));
        }

        return p;
    }

    private byte[] toBytes(Object o) {
        if (o instanceof Integer) {
            return toBytes((Integer)o);
        } else if (o instanceof Double) {
            return toBytes((Double) o);
        } else {
            return toBytes((String) o);
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