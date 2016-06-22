package speed.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.windowing.TupleWindow;
import java.io.Serializable;
import java.util.Map;


/**
 * Created by nickozoulis on 12/06/2016.
 */
public abstract class PookaInputBolt extends BaseWindowedBolt implements Serializable {
    private static final long serialVersionUID = -2268993895150431399L;
    private OutputCollector collector;
    private Long window;

    public PookaInputBolt(Long window) {
        this.window = window;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public abstract void execute(TupleWindow inputWindow);

    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

    public void incrementWindow() {
        this.window++;
    }

    protected OutputCollector getCollector() {
        return this.collector;
    }
    public Long getWindow() {
        return window;
    }

}
