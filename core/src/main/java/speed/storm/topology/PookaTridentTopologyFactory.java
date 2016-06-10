package speed.storm.topology;

import speed.storm.spout.KafkaSpoutFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.trident.TridentTopology;

import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
public class PookaTridentTopologyFactory extends TopologyFactory {
    private TridentTopology tridentTopology;

    public PookaTridentTopologyFactory(Properties properties) {
        tridentTopology = new TridentTopology();

        Object spout =  new KafkaSpoutFactory(properties).getSpout();
        if (spout instanceof OpaqueTridentKafkaSpout) {
            OpaqueTridentKafkaSpout tridentKafkaSpout = (OpaqueTridentKafkaSpout) spout;

           //TODO:
        }
    }

    @Override
    TridentTopology getTopology() {
        return tridentTopology;
    }
}
