package speed.storm.bolt;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import java.io.Serializable;
import java.util.Map;


/**
 * Created by nickozoulis on 23/06/2016.
 */
public abstract class PookaWindow extends BaseWindowedBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(PookaWindow.class);
    private static final long serialVersionUID = -4326159306720476874L;
    private OutputCollector collector;
    private Long window;
    protected int TASK_ID;

    public PookaWindow(Long window) {
        this.window = window;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.TASK_ID = context.getThisTaskId();
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        for (Tuple tuple : inputWindow.get()) {
            logger.info("Emitting normal tuple for window: " + getWindow());
            execute(tuple);
        }
        // Special msg indicate end of window
        logger.info("Emitting ack tuple for window: " + getWindow());
        emitAckTuple();
        incrementWindow();
    }

    public void incrementWindow() {
        logger.info("Incrementing window of task: " + TASK_ID);
        this.window++;
        logger.info("Incremented window Id: " + window);
    }

    protected abstract void emitAckTuple();

    protected abstract void execute(Tuple tuple);

    @Override
    public abstract void declareOutputFields(OutputFieldsDeclarer declarer);

    @Override
    public void cleanup() {
    }

    public OutputCollector getCollector() {
        return this.collector;
    }

    public Long getWindow() {
        return window;
    }

}
