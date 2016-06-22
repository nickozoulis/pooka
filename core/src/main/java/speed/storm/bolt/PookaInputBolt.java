package speed.storm.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import java.io.Serializable;
import java.util.Map;


/**
 * Created by nickozoulis on 12/06/2016.
 */
public abstract class PookaInputBolt extends BaseRichBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(PookaInputBolt.class);
    private static final long serialVersionUID = -2268993895150431399L;
    private OutputCollector collector;
    private Long window;
    protected int TASK_ID;

    public PookaInputBolt(Long window) {
        this.window = window;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.TASK_ID = context.getThisTaskId();
    }

    @Override
    public abstract void execute(Tuple input);

    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

    public void incrementWindow() {
        logger.info("Incrementing window of task: " + TASK_ID);
        this.window++;
        logger.info("Incremented window Id: " + window);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        int tickFrequencyInSeconds = 10;
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickFrequencyInSeconds);
        return conf;
    }

    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
                && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

    protected OutputCollector getCollector() {
        return this.collector;
    }
    public Long getWindow() {
        return window;
    }

}
