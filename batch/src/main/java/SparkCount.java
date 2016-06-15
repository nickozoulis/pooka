import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;

import java.io.IOException;
import java.util.List;

/**
 * Created by nickozoulis on 14/06/2016.
 */
public class SparkCount {
    private static HTable tableSpeed, tableRaw;
    private static Long speedLastTimestamp;
    private static Configuration hBaseConfig;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Count").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            hBaseConfig = Utils.setHBaseConfig();

            tableSpeed = new HTable(hBaseConfig, Cons.TABLE_SPEED);
            tableRaw = new HTable(hBaseConfig, Cons.MASTER_DATASET);

            tableRaw.setAutoFlush(Cons.AUTO_FLUSH, Cons.CLEAR_BUFFER_ON_FAIL);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // Scan Speed table to get last entry's timestamp
            Scan scan = new Scan(Bytes.toBytes(1l));
            scan.addFamily(Cons.COLUMN_FAMILY_SPEED.getBytes());
            scan.setReversed(true);

            ResultScanner rs = tableSpeed.getScanner(scan);

            for (Result r : rs) {
                speedLastTimestamp = new Long(r.raw()[0].getTimestamp());
                break;
            }
            System.out.println("><><><><><> " + speedLastTimestamp);

            // Scan Master Dataset table to start batch processing
            scan = new Scan();
            scan.setTimeRange(1, speedLastTimestamp);

            hBaseConfig.set(TableInputFormat.INPUT_TABLE, Cons.TABLE_SPEED);
            hBaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan));

            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                    hBaseConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            JavaRDD<String> studentRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, String  >() {
                private static final long serialVersionUID = -2021713021648730786L;
                public String  call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                    String s = null;
                    try {
                        Result result = tuple._2;

                        s = Bytes.toString(result.getRow());

                        return s;
                    } catch(Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });

            List<String> list = studentRDD.collect();

            for (String s : list)
                System.out.println("??????? " + s);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    private static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }
}
