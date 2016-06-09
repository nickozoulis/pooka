package source.kafka;

/**
 * Created by nickozoulis on 09/06/2016.
 */
public class KafkaProducerFactory extends ProducerFactory {
    @Override
    Producer getProducer() {
        return new CustomKafkaProducer();
    }
}
