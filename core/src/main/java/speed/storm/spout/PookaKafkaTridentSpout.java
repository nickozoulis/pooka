package speed.storm.spout;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.TridentTopology;

import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
class PookaKafkaTridentSpout implements SpoutClient<OpaqueTridentKafkaSpout> {
    private OpaqueTridentKafkaSpout spout;

    //TODO:
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
