package speed.storm.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import java.util.Map;

/**
 * Created by nickozoulis on 12/06/2016.
 */
public class TumblingWindowBolt extends BaseWindowedBolt {
    private OutputCollector collector;
    private Long windowId;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        windowId = System.currentTimeMillis();

        for (Tuple tuple : inputWindow.get()) {
            collector.emit(new Values(tuple, windowId));
            collector.ack(tuple);
        }
        // Dummy tuple to indicate end of window
        collector.emit(new Values("", -1L));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tuple", "windowId"));
    }
}
