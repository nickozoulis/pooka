package speed.storm.bolt;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by nickozoulis on 22/06/2016.
 */
public class PookaBundle implements Serializable {
    private static final long serialVersionUID = -3995412090331338480L;
    private Map<Long, PookaView> viewMap;
    private Map<Long, List<Put>> rawPuts;
    private Map<Long, Integer> acks;
    private final int numOfInputBolts;

    public PookaBundle(int numOfInputBolts, HTable tableRaw, HTable tableSpeed) {
        this.numOfInputBolts = numOfInputBolts;
        setViewMap(new HashMap<Long, PookaView>());
        setRawPuts(new HashMap<Long, List<Put>>());
        acks = new HashMap<>();
    }

    public boolean processAck(Long window) {
        acks.put(window, acks.get(window) + 1);

        return acks.get(window) == numOfInputBolts;
    }

    public void removeFromBundle(Long window) {
        getViewMap().remove(window);
        getRawPuts().remove(window);
        acks.remove(window);
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
}
