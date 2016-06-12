
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import speed.storm.bolt.SplitBolt;
import speed.storm.bolt.TumblingWindowBolt;
import speed.storm.spout.PookaKafkaCoreSpout;

import java.util.Properties;


/**
 * Created by nickozoulis on 11/06/2016.
 */
public class TestMain {

    public static void main(String[] args) throws InterruptedException {
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setDebug(true);

        Properties p = new Properties();
        p.put("zkConnString", "localhost:2181");
        p.put("topic", "test");
        p.put("zkNamespace", "test_kafka");

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new PookaKafkaCoreSpout(p).getSpout());
        builder.setBolt("word-spitter", new SplitBolt()).shuffleGrouping("kafka-spout");
        builder.setBolt("word-counter", new TumblingWindowBolt()).fieldsGrouping("word-spitter", new Fields("word"));


        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStormSample", conf, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("KafkaStormSample");
        cluster.shutdown();
    }

}
