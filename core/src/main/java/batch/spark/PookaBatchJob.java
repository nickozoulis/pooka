package batch.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import serving.hbase.TimestampNotFoundException;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public abstract class PookaBatchJob implements Serializable {
    private static final long serialVersionUID = -3258884693833351600L;
    private Long speedLastTimestamp;
    private transient JavaSparkContext sc;
    private transient Configuration hBaseConfig;
    private JavaRDD<String> batchRDD;
    private Long startTS, endTS;

    protected PookaBatchJob(String appName, String mode, Function hbaseMapper, Long startTS, Long endTS) {
        setStartTS(startTS);
        setEndTS(endTS);

        SparkConf conf = new SparkConf().setAppName(appName).setMaster(mode);
        sc = new JavaSparkContext(conf);

        try {
            hBaseConfig = Utils.setHBaseConfig();

            HTable tableSpeed = new HTable(hBaseConfig, Cons.TABLE_SPEED);

            // Get most recent timestamp from speed views
            speedLastTimestamp = loadSpeedLastTimestamp(tableSpeed);

            batchRDD = loadBatchRDD(hbaseMapper);

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

    private JavaRDD<String> loadBatchRDD(Function hbaseMapper) throws IOException {
        // Scan master dataset table to start batch processing
        Scan scan = new Scan();
        scan.setTimeRange(startTS, endTS);   // +1 because upper bound is exclusive

        hBaseConfig.set(TableInputFormat.INPUT_TABLE, Cons.MASTER_DATASET);
        hBaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan));

        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRDD = sc.newAPIHadoopRDD(
                hBaseConfig, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

        JavaRDD batchRDD = hbaseRDD.map(hbaseMapper);

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
        scan.addFamily(Cons.CF_VIEWS.getBytes());
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

    protected void setStartTS(Long startTS) {
        this.startTS = startTS;
    }

    protected void setEndTS(Long endTS) {
        this.endTS = endTS;
    }

}
