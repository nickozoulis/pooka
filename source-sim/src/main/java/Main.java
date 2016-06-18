import source.kafka.KafkaProducerFactory;
import source.kafka.IProducer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by nickozoulis on 18/06/2016.
 */
public class Main {
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("First argument should be the topic and second should be the input filepath");
            System.exit(0);
        }

        Properties properties = new Properties();
        properties.setProperty("topic", args[0]);
        
        KafkaProducerFactory factory = new KafkaProducerFactory(properties);
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
