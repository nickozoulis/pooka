package speed.storm.spout;

import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
public class KafkaSpoutFactory extends SpoutFactory {
    private Properties properties;

    public KafkaSpoutFactory(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Object getSpout() {
        String type = properties.getProperty("spout-type");
        Object o = new Object();

        if (type.equals("kafka-core-spout")) {
            o = new PookaKafkaCoreSpout(properties).getSpout();
        } else if (type.equals("kafka-trident-spout")) {
            o = new PookaKafkaTridentSpout(properties).getSpout();
        }

        return o;
    }
}
