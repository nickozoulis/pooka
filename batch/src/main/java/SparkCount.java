import batch.spark.operators.CountUrlIds;
import batch.spark.operators.ExtractUrlIds;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import serving.hbase.TimestampNotFoundException;
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
            speedLastTimestamp = getSpeedLastTimestamp(tableSpeed);
            System.out.println("><><><><><> " + speedLastTimestamp);

            // Scan Master Dataset table to start batch processing
            Scan scan = new Scan();
            // +1 because upper bound is exclusive
            scan.setTimeRange(1, speedLastTimestamp + 1);

            hBaseConfig.set(TableInputFormat.INPUT_TABLE, Cons.MASTER_DATASET);
            hBaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan));

            JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                    hBaseConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            JavaRDD<String> batchRDD = hbaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, String>() {
                private static final long serialVersionUID = -2021713021648730786L;

                public String call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
                    String s = null;
                    try {
                        Result result = tuple._2;

                        s = Bytes.toString(result.getRow());

                        return s;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            });

            // -- Start batch processing

            JavaPairRDD<String, Integer> pairs = batchRDD.mapToPair(new ExtractUrlIds());
            JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CountUrlIds());

            // -- Write to HBase

            Job job = Job.getInstance(hBaseConfig);
            job.setOutputFormatClass(TableOutputFormat.class);
//            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
//            job.setMapOutputValueClass(Put.class);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Cons.TABLE_BATCH);

            // Map rdd data schema to hbase schema
            final Long batchTimestamp = speedLastTimestamp;

            JavaPairRDD<ImmutableBytesWritable, Put> hbasePuts = counters.mapToPair(
                    new PairFunction<Tuple2<String, Integer>, ImmutableBytesWritable, Put>() {
                @Override
                public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Integer> pair) throws Exception {

                    Put put = new Put(Bytes.toBytes(batchTimestamp), batchTimestamp);
                    put.addColumn(Bytes.toBytes(Cons.CF_BATCH), Bytes.toBytes(pair._1()), Bytes.toBytes(pair._2()));

                    return new Tuple2<>(new ImmutableBytesWritable(), put);
                }
            });

            hbasePuts.saveAsNewAPIHadoopDataset(job.getConfiguration());

//            List<Tuple2<String, Integer>> list = counters.collect();
//
//            for (Tuple2 t : list) {
//                System.out.println("??????? " + t._1() + " -> " + t._2());
//            }


        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    private static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    private static Long getSpeedLastTimestamp(HTable tableSpeed) throws IOException, TimestampNotFoundException {
        Long speedLastTimestamp = null;

        // Scan Speed table to get last entry's timestamp
        Scan scan = new Scan();
        scan.addFamily(Cons.CF_SPEED.getBytes());
        scan.setReversed(true);

        ResultScanner rs = tableSpeed.getScanner(scan);

        for (Result r : rs) {
            speedLastTimestamp = new Long(r.raw()[0].getTimestamp());
            break;
        }

        if (speedLastTimestamp == null) {
            throw new TimestampNotFoundException();
        }

        return speedLastTimestamp;
    }

}
