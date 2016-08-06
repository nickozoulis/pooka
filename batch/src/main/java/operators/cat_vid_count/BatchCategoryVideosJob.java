package operators.cat_vid_count;

import batch.spark.PookaBatchJob;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchCategoryVideosJob extends PookaBatchJob implements Serializable {
    private static final Logger logger = Logger.getLogger(BatchCategoryVideosJob.class);
    private static final long serialVersionUID = 3420047706448356615L;

    public BatchCategoryVideosJob(String appName, String mode) {
        super(appName, mode, new CategoryMapper());
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryPair());
        JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CategoryVideosCounter());

        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new BatchResultToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        while (true) {
            new BatchCategoryVideosJob("Count", "local").start();
        }
    }

}
