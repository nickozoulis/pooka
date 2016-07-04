package cat_stdev_views;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import serving.hbase.Utils;
import speed.storm.spout.PookaKafkaSpout;
import utils.SplitBolt;
import utils.WindowBolt;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by nickozoulis on 04/07/2016.
 */
public class Main {

    public static void main(String[] args) throws InterruptedException {
        // k = Num of input (window) bolts, n = Num of output (flush) bolts
        // k << n
        int k, n, t;
        if (args.length == 3) {
            k = Integer.parseInt(args[0]);
            n = Integer.parseInt(args[1]);
            t = Integer.parseInt(args[2]);
        } else {
            System.out.println("Setting default values for parallelism, k = 1, n = 1, t = 10");
            k = 1;
            n = 1;
            t = 10;
        }

        Utils.deleteAllSchemaTables();
        Utils.createAllSchemaTables();

        Config conf = new Config();
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        conf.setDebug(true);

        Properties p = new Properties();
        p.put("zkConnString", "localhost:2181");
        p.put("topic", "youtube");
        p.put("zkNamespace", "youtube_kafka");

        // Initial value of the window that all input bolts will be fed with.
        final Long initWindow = System.currentTimeMillis();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", new PookaKafkaSpout(p).getSpout());
        builder.setBolt("word-spitter", new SplitBolt())
                .setNumTasks(n)
                .shuffleGrouping("kafka-spout");
        builder.setBolt("window-creator", new WindowBolt(initWindow)
                .withTumblingWindow(new BaseWindowedBolt.Duration(t, TimeUnit.SECONDS)))
                .setNumTasks(k)
                .shuffleGrouping("word-spitter");
        builder.setBolt("avg-counter", new StdevCatViewsBolt(k))
                .setNumTasks(n)
                .fieldsGrouping("window-creator", new Fields("window"));

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("KafkaStormSample", conf, builder.createTopology());
        Thread.sleep(60000);
        cluster.killTopology("KafkaStormSample");
        cluster.shutdown();
    }

}
