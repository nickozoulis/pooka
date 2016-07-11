package spark_streaming;

import kafka.serializer.StringDecoder;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;
import serving.hbase.Utils;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by nickozoulis on 08/07/2016.
 */
public class Main implements Serializable {
    private static final Logger logger = Logger.getLogger(Main.class);
    private static final long serialVersionUID = 3184664379188037699L;
    private static Long initWindow = System.currentTimeMillis();

    public static void main(String[] args) throws IOException {
        Utils.deleteAllSchemaTables();
        Utils.createAllSchemaTables();

        SparkConf conf = new SparkConf().setAppName("SparkStreaming").setMaster("local[2]");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(10));

        String topics = "youtube";
        String brokers = "localhost:9092";

        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", brokers);

        /*
        maybe try not-direct receiver
         */
        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        // Get the lines, split them into words, count the words and print
        JavaDStream<String> categories = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                String line = tuple2._2();

                String[] splits = line.split("\\t");

                return splits[3]; // category
            }
        });

        logger.info(">>>>>>>>>>>> categories: "+categories.count());

        /*
        Na to allaxo, na exo mia global variable san window kai na to ksekiniso apo ton driver kai na ton doso
        se ola ta rdd. Kai ayta tha auxanoun monotona to parathuro kai tha kanoun put stin hbase.
         */
        JavaPairDStream<String, Long> counts = categories.countByValue();
        logger.info(">>>>>>>>>>>> counts: "+counts.count());
        // thelo na kano to javapairdstream -> javapairrdd gia na efarmoso : saveAsNewAPIHadoopDataset

        /*
         * Workaround to store view in HBase, because javapairdstream has not saveAsNewAPIHadoopDataset()
         */

        counts.foreachRDD(new CountsAdapter(initWindow++));

        // Start the computation
        jssc.start();
        jssc.awaitTermination();
    }

}
