package speed.storm.spout;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.*;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by nickozoulis on 10/06/2016.
 */
class PookaKafkaCoreSpout implements Spout<KafkaSpout> {
    private KafkaSpout kafkaSpout;

    public PookaKafkaCoreSpout(Properties properties) {
        BrokerHosts hosts = new ZkHosts(properties.getProperty("zkConnString"));
        SpoutConfig spoutConfig = new SpoutConfig(hosts, properties.getProperty("topic"),
                "/" + properties.getProperty("zkNamespace"),
                UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaSpout = new KafkaSpout(spoutConfig);
    }

    public KafkaSpout getSpout() {
        return kafkaSpout;
    }
}
