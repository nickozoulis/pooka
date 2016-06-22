package speed.storm;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import speed.storm.bolt.Cons;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by nickozoulis on 22/06/2016.
 */
public class PookaBundle implements Serializable {
    private Map<Long, PookaView> viewMap;
    private Map<Long, List<Put>> rawPuts;
    private Map<Long, Integer> acks;
    private final int numOfInputBolts;
    // HBase variables
    private HTable tableSpeed, tableRaw;
    private static boolean AUTO_FLUSH = false;
    private static boolean CLEAR_BUFFER_ON_FAIL = false;

    public PookaBundle(int numOfInputBolts) {
        this.numOfInputBolts = numOfInputBolts;
        setViewMap(new HashMap<Long, PookaView>());
        setRawPuts(new HashMap<Long, List<Put>>());
        setAcks(new HashMap<Long, Integer>());
    }

    public void processAck(Long window) {
        getAcks().put(window, getAcks().get(window) + 1);

        if (getAcks().get(window) == numOfInputBolts) {
            flush(window);
            removeFromBundle(window);
        }
    }

    private void removeFromBundle(Long window) {
        getViewMap().remove(window);
        getRawPuts().remove(window);
        getAcks().remove(window);
    }

    private void flush(Long window) {
        try {
            // Write raw data to master dataset in HBase.
            tableRaw.put(getRawPuts().get(window));
            // Write speed views to speed view table in HBase.
            writeSpeedViewToHBase(window);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeSpeedViewToHBase(Long window) throws IOException {
        Put p = new Put(Bytes.toBytes(window), window);
        for (Map.Entry<String, Integer> entry : ((MyView) getViews().get(window)).getViews().entrySet()) {
            p.addColumn(Cons.CF_VIEWS.getBytes(), toBytes("count_" + entry.getKey()), toBytes(entry.getValue()));
        }
        tableSpeed.put(p);
    }

    private byte[] toBytes(String b) {
        return Bytes.toBytes(b);
    }

    private byte[] toBytes(Integer b) {
        return Bytes.toBytes(b);
    }

    private byte[] toBytes(Double b) {
        return Bytes.toBytes(b);
    }

    public Map<Long, PookaView> getViewMap() {
        return viewMap;
    }

    private void setViewMap(Map<Long, PookaView> viewMap) {
        this.viewMap = viewMap;
    }

    public Map<Long, List<Put>> getRawPuts() {
        return rawPuts;
    }

    private void setRawPuts(Map<Long, List<Put>> rawPuts) {
        this.rawPuts = rawPuts;
    }

    public Map<Long, Integer> getAcks() {
        return acks;
    }

    private void setAcks(Map<Long, Integer> acks) {
        this.acks = acks;
    }
}
