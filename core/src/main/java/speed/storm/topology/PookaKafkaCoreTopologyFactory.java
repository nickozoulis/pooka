package speed.storm.topology;

import backtype.storm.topology.TopologyBuilder;
import speed.storm.spout.KafkaSpoutFactory;
import storm.kafka.KafkaSpout;
import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
public class PookaKafkaCoreTopologyFactory extends TopologyFactory {
    private TopologyBuilder topologyBuilder;

    public PookaKafkaCoreTopologyFactory(Properties properties) {
        topologyBuilder = new TopologyBuilder();

        Object spout =  new KafkaSpoutFactory(properties).getSpout();

        if (spout instanceof KafkaSpout) {
            KafkaSpout kafkaSpout = (KafkaSpout)spout;

            topologyBuilder.setSpout(properties.getProperty("spout-name"), kafkaSpout);
//            topologyBuilder.setBolt(properties.getProperty("bolt-name"), );
        }
    }

    @Override
    TopologyBuilder getTopology() {
        return topologyBuilder;
    }
}
