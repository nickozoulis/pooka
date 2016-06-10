package source.kafka;

import java.util.Properties;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public class KafkaProducerFactory extends ProducerFactory {
    private Properties properties;

    public KafkaProducerFactory(Properties properties) {
        this.properties = properties;
    }

    @Override
    Producer getProducer() {
        return new PookaKafkaProducer(properties);
    }
}
