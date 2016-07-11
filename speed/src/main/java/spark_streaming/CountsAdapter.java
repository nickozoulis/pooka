package spark_streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Time;
import scala.Tuple2;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;
import java.io.IOException;

/**
 * Created by nickozoulis on 09/07/2016.
 */
public class CountsAdapter implements Function2<JavaPairRDD<String, Long>, Time, Void> {
    private static final Logger logger = Logger.getLogger(CountsAdapter.class);
    private static final long serialVersionUID = -856059955003989765L;
    private Long window;
    private transient Put p = null;
    private transient HTable tableSpeed;
    private boolean AUTO_FLUSH = false;
    private boolean CLEAR_BUFFER_ON_FAIL = false;

    public CountsAdapter(Long window) {
        this.window = window;
        try {
            Configuration conf = Utils.setHBaseConfig();
            tableSpeed = new HTable(conf, Cons.TABLE_SPEED);
            tableSpeed.setAutoFlush(AUTO_FLUSH, CLEAR_BUFFER_ON_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Void call(JavaPairRDD<String, Long> v1, Time v2) throws Exception {
        p = new Put(Bytes.toBytes(window), window);
        logger.info(">>>>>>>>> call: " + v1.count());
        v1.foreach(new VoidFunction<Tuple2<String, Long>>() {
            private static final long serialVersionUID = -880537073776733103L;

            @Override
            public void call(Tuple2<String, Long> tuple) throws Exception {
                p.addColumn(Cons.CF_VIEWS.getBytes(), Bytes.toBytes(tuple._1()), Bytes.toBytes(tuple._2() + ""));
                System.out.println("<<<<<<<<<<< tuple: " + tuple._1() + " " + tuple._2());
            }
        });

        tableSpeed.put(p);
        tableSpeed.close();
        return null;
    }
}