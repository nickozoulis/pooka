package source.kafka;

import java.util.Properties;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public class KafkaConsumerFactory extends ConsumerFactory {
    private Properties properties;
    private String topic, groupId;

    public KafkaConsumerFactory(Properties properties, String topic, String groupId) {
        this.properties = properties;
        this.topic = topic;
        this.groupId = groupId;
    }

    @Override
    public Consumer getConsumer() {
        return new CustomKafkaConsumer(properties, topic, groupId);
    }
}
