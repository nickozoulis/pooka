import source.kafka.KafkaProducerFactory;
import source.kafka.IProducer;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;

/**
 * Created by nickozoulis on 18/06/2016.
 */
public class Main {
    // %s = string category, %d = int # of video views
    private static String template = "LKh7zAJ4nwo\tTheReceptionist\t653\t%s\t424\t%d\t4.34\t1305\t744\tDjdA-5oKYFQ\tNxTDlnOuybo\tc-8VuICzXtU\tDH56yrIO5nI\tW1Uo5DQTtzc\tE-3zXq_r4w0\t1TCeoRPg5dE\tyAr26YhuYNY\t2ZgXx72XmoE\t-7ClGo-YgZ0\tvmdPOOd6cxI\tKRHfMQqSHpk\tpIMpORZthYw\t1tUDzOp10pk\theqocRij5P0\t_XIuvoH6rUg\tLGVU5DsezE0\tuO2kj6_D8B4\txiDqywcDQRM\tuX81lMev6_o";
    private static String[] categories = {"Entertainment", ""};
    private static Random r = new Random(1);

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.out.println("First argument should be the topic and second the upper limit of string generator loop.");
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

        int upper = Integer.parseInt(args[1]);

        for (int i=0; i<upper; i++) {
            producer.send(strGenerator());

            if (i % 1000 == 0) {
                System.out.println(">>> " + i);
            }
        }

    }

    private static String strGenerator() {
        String category = categories[r.nextInt(categories.length - 1)];
        int views = r.nextInt(10000000);

        return String.format(template, category, views);
    }
}
