package source.kafka;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import java.util.Map;
import java.util.Properties;

/**
 * Created by nickozoulis on 09/06/2016.
 */
class PookaKafkaProducer implements source.kafka.Producer<String, String> {
    private Properties properties;
    private ProducerConfig config;
    private Producer<String, String> producer;
    private KeyedMessage<String, String> data;

    public PookaKafkaProducer(Properties properties) {
        this.properties = properties;
        config = new kafka.producer.ProducerConfig(properties);
    }

    @Override
    public void open() {
        producer = new Producer<>(config);
    }

    @Override
    public void send(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            data = new KeyedMessage<>(properties.getProperty("topic"), key, value);
            producer.send(data);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
