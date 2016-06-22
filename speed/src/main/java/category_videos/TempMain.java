package category_videos;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import speed.storm.spout.PookaKafkaSpout;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by nickozoulis on 23/06/2016.
 */
public class TempMain {
    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setDebug(true);

        Properties p = new Properties();
        p.put("zkConnString", "localhost:2181");
        p.put("topic", "youtube");
        p.put("zkNamespace", "youtube_kafka");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new PookaKafkaSpout(p).getSpout());
        builder.setBolt("word-spitter", new TempBolt()).shuffleGrouping("kafka-spout");



        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStormSample", conf, builder.createTopology());
        Thread.sleep(100000);
        cluster.killTopology("KafkaStormSample");
        cluster.shutdown();
    }
}
