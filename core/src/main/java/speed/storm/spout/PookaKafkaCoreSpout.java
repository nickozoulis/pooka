package speed.storm.spout;



import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by nickozoulis on 10/06/2016.
 */
public class PookaKafkaCoreSpout implements SpoutClient<KafkaSpout> {
    /*
        KafkaSpout is our spout implementation, which will integrate with Storm. It fetches the mes-sages from kafka
        topic and emits it into Storm ecosystem as tuples. KafkaSpout get its config-uration details from SpoutConfig.
     */
    private KafkaSpout kafkaSpout;

    public PookaKafkaCoreSpout(Properties properties) {
        /*
            BrokerHosts is an interface and ZkHosts and StaticHosts are its two main implementations.
            ZkHosts is used to track the Kafka brokers dynamically by maintaining the details in ZooKeeper
         */
        BrokerHosts hosts = new ZkHosts(properties.getProperty("zkConnString"));
        /*
            This API is used to define configuration settings for the Kafka cluster.
            Spoutconfig is an extension of KafkaConfig that supports additional ZooKeeper information.
         */
        SpoutConfig spoutConfig = new SpoutConfig(
                hosts,
                properties.getProperty("topic"),
                "/" + properties.getProperty("zkNamespace"),
                UUID.randomUUID().toString());
        /*
            SchemeAsMultiScheme is an interface that dictates how the ByteBuffer consumed from Kafka gets transformed
            into a storm tuple. One implementation, StringScheme, parses the byte as a simple string.
            It also controls the naming of your output field.
         */
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        kafkaSpout = new KafkaSpout(spoutConfig);
    }

    public KafkaSpout getSpout() {
        return kafkaSpout;
    }
}
