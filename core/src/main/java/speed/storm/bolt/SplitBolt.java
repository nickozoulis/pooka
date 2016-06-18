package speed.storm.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by nickozoulis on 11/06/2016.
 */
public class SplitBolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = -2784425596889772745L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
            String sentence = input.getString(0);
            String[] words = sentence.split(" ");

            for (String word : words) {
                word = word.trim();

                if (!word.isEmpty()) {
                    word = word.toLowerCase();
                    collector.emit(new Values(sentence, word));
                }
            }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence", "word"));
    }

}
