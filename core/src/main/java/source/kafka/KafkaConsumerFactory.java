package source.kafka;

import java.util.Properties;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public class KafkaConsumerFactory extends ConsumerFactory {
    private Properties properties;

    public KafkaConsumerFactory(Properties properties) {
        this.properties = properties;
    }

    @Override
    public Consumer getConsumer() {
        return new CustomKafkaConsumer(properties);
    }
}
