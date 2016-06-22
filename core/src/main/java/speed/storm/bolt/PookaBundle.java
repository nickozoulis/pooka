package speed.storm.bolt;

import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by nickozoulis on 22/06/2016.
 */
public class PookaBundle implements Serializable {
    private static final Logger logger = Logger.getLogger(PookaBundle.class);
    private static final long serialVersionUID = -3995412090331338480L;
    private Map<Long, PookaView> viewMap;
    private Map<Long, List<Put>> rawPuts;
    private Map<Long, Integer> acks;
    private final int numOfInputBolts;

    public PookaBundle(int numOfInputBolts) {
        this.numOfInputBolts = numOfInputBolts;
        setViewMap(new HashMap<Long, PookaView>());
        setRawPuts(new HashMap<Long, List<Put>>());
        acks = new HashMap<>();
    }

    public boolean processAck(Long window) {
        logger.info("Processing ack tuple of window: " + window);
        acks.put(window, acks.get(window) + 1);
        logger.info("Total acks: " + acks.get(window));
        return acks.get(window) == numOfInputBolts;
    }

    public void removeFromBundle(Long window) {
        getViewMap().remove(window);
        getRawPuts().remove(window);
        acks.remove(window);
    }

    public Map<Long, Integer> getAcks() {
        return acks;
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
