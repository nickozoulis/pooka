package source.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import java.util.*;

/**
 * Created by nickozoulis on 09/06/2016.
 */
class PookaKafkaConsumer implements IConsumer {
    private Properties properties;
    private final KafkaConsumer<String, String> consumer;

    public PookaKafkaConsumer(Properties properties) {
        this.properties = properties;
        this.consumer = new KafkaConsumer<>(properties);
    }

    @Override
    public void open() {
        this.consumer.subscribe(Arrays.asList(properties.getProperty("topic")));
    }

    @Override
    public Iterator fetch() {
        return this;
    }

    @Override
    public void close() {
        consumer.wakeup();
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Map<String, Object> next() {
        Map<String, Object> data = new HashMap<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
            for (ConsumerRecord<String, String> record : records) {
                data.put("partition", record.partition());
                data.put("offset", record.offset());
                data.put("value", record.value());
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }

        return data;
    }

    @Override
    public void remove() {

    }
}
