package speed.storm.bolt;



import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 11/06/2016.
 */
public class CountBolt extends BaseRichBolt {
    private OutputCollector collector;
    private Map<String, Integer> counters;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counters = new HashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String str = input.getString(0);

        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }
}
