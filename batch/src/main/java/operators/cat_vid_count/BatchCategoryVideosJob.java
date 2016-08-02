package operators.cat_vid_count;

import batch.spark.PookaBatchJob;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchCategoryVideosJob extends PookaBatchJob implements Serializable {
    private static final Logger logger = Logger.getLogger(BatchCategoryVideosJob.class);
    private static final long serialVersionUID = 3420047706448356615L;

    public BatchCategoryVideosJob(String appName, String mode, Long startTS, Long endTS) {
        super(appName, mode, new CategoryMapper(), startTS, endTS);
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryPair());
        System.out.println(">>>> count pairs of batch rdd" + pairs.count());
        JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CategoryVideosCounter());
        System.out.println(">>>> count pairs of counters" + counters.count());

        System.out.println(">>>>>> Finished DAG");
        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new BatchResultToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        if (args.length != 2) System.exit(1);

        Long startTime = System.currentTimeMillis();

        Long startTS = Long.parseLong(args[0]);
        Long endTS = Long.parseLong(args[1]);

        new BatchCategoryVideosJob("Count", "local", startTS, endTS).start();
        Long endTime = System.currentTimeMillis();

        logger.info(Math.abs(endTime-startTime));
    }

}
