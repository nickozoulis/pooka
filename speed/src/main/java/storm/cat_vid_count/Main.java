package storm.cat_vid_count;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import serving.hbase.Utils;
import speed.storm.spout.PookaKafkaSpout;
import storm.utils.SplitBolt;
import storm.utils.WindowBolt;

import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * Created by nickozoulis on 11/06/2016.
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
        builder.setBolt("word-counter", new CountCategoryViewsBolt(k))
                .setNumTasks(n)
                .fieldsGrouping("window-creator", new Fields("window"));

//        LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("KafkaStormSample", conf, builder.createTopology());

        System.setProperty("storm.jar", "/home/nickoszoulis/speed-1.0-SNAPSHOT-all.jar");
        try {
            StormSubmitter.submitTopology("CountCategoryTopology", conf, builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }

    }

}
