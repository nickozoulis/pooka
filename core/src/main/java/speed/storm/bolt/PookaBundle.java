package speed.storm.bolt;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
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

    public PookaBundle(int numOfInputBolts, HTable tableRaw, HTable tableSpeed) {
        this.numOfInputBolts = numOfInputBolts;
        setViewMap(new HashMap<Long, PookaView>());
        setRawPuts(new HashMap<Long, List<Put>>());
        setAcks(new HashMap<Long, Integer>());
    }

    public boolean processAck(Long window) {
        getAcks().put(window, getAcks().get(window) + 1);

        return (getAcks().get(window) == numOfInputBolts) ? true : false;
    }

    public void removeFromBundle(Long window) {
        getViewMap().remove(window);
        getRawPuts().remove(window);
        getAcks().remove(window);
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
