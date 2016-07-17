import source.kafka.KafkaProducerFactory;
import source.kafka.IProducer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by nickozoulis on 18/06/2016.
 */
public class FileFeeder {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("First argument should be the topic and second should be the input filepath");
            System.exit(0);
        }

        Properties p = new Properties();
        p.put("bootstrap.servers", "localhost:9092"); // Assign localhost id
        p.put("acks", "all");  // Set acknowledgements for producer requests.
        p.put("retries", 0);    // If the request fails, the producer can automatically retry
        p.put("batch.size", 16384); // Specify buffer size in config
        // The buffer.memory controls the total amount of memory available to the producer for buffering.
        p.put("buffer.memory", 33554432);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        p.setProperty("topic", args[0]);

        KafkaProducerFactory factory = new KafkaProducerFactory(p);
        IProducer producer = factory.getProducer();

        BufferedReader br = null;
        String line = "";
        try {
            br = new BufferedReader(new FileReader(args[1]));

            while ((line = br.readLine()) != null) {
                producer.send(line);
            }
        } catch (Exception e) {
        } finally {
            producer.close();
            br.close();
        }
    }
}
