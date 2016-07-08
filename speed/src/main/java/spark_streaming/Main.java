package spark_streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * Created by nickozoulis on 08/07/2016.
 */
public class Main {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("local[1]").setMaster("SparkStreaming");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(10));
    }

}
