package batch.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
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

/**
 * Created by nickozoulis on 17/06/2016.
 */
public abstract class PookaBatchJob {
    private Long speedLastTimestamp;
    private JavaSparkContext sc;
    private Configuration hBaseConfig;
    private JavaRDD<String> batchRDD;

    public PookaBatchJob(String appName, String mode) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(mode);
        sc = new JavaSparkContext(conf);

        try {
            hBaseConfig = Utils.setHBaseConfig();

            HTable tableSpeed = new HTable(hBaseConfig, Cons.TABLE_SPEED);

            // Get most recent timestamp from speed views
            speedLastTimestamp = loadSpeedLastTimestamp(tableSpeed);

            batchRDD = loadBatchRDD();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void start() {
        try {
            Job job = Job.getInstance(hBaseConfig);
            job.setOutputFormatClass(TableOutputFormat.class);
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, Cons.TABLE_BATCH);
            DAG().mapToPair(hbaseSchemaAdapter()).saveAsNewAPIHadoopDataset(job.getConfiguration());
        } catch (IOException e) {e.printStackTrace();}
    }

    public abstract JavaPairRDD DAG ();

    public abstract PairFunction hbaseSchemaAdapter();

    private JavaRDD<String> loadBatchRDD() throws IOException {
        // Scan master dataset table to start batch processing
        Scan scan = new Scan();
        scan.setTimeRange(1, speedLastTimestamp + 1);   // +1 because upper bound is exclusive

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

        return batchRDD;
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    private String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    private Long loadSpeedLastTimestamp(HTable tableSpeed) throws IOException, TimestampNotFoundException {
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

    protected JavaRDD<String> getBatchRDD() {
        return this.batchRDD;
    }

    protected Long getBatchTimestamp() {
        return this.speedLastTimestamp;
    }

}
