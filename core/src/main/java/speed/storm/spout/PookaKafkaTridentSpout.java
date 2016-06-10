package speed.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
public class PookaKafkaTridentSpout implements Spout<OpaqueTridentKafkaSpout> {
    private OpaqueTridentKafkaSpout spout;

    public PookaKafkaTridentSpout(Properties properties) {
        TridentTopology topology = new TridentTopology();
        BrokerHosts zk = new ZkHosts(properties.getProperty("host"));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, properties.getProperty("topic"));
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spout = new OpaqueTridentKafkaSpout(spoutConf);
    }

    public OpaqueTridentKafkaSpout getSpout() {
        return spout;
    }

}
