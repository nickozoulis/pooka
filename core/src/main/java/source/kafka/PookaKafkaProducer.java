package source.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

/**
 * Created by nickozoulis on 09/06/2016.
 */
class PookaKafkaProducer implements IProducer<String, String> {
    private Properties properties;
    private Producer<Object, String> producer;
    private String topic;

    public PookaKafkaProducer(Properties properties) {
        this.properties = properties;
        this.topic = properties.getProperty("topic");
        open();
    }

    @Override
    public void open() {
        producer = new KafkaProducer<>(properties);
    }

    @Override
    public void send(String key, String value) {}

    @Override
    public void send(String value) {
        producer.send(new ProducerRecord<>(topic, value));
    }

    @Override
    public void close() {
        producer.close();
    }
}
