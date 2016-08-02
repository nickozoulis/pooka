package operators.cat_stdev_views;

import batch.spark.PookaBatchJob;
import operators.utils.CategoryViewsMapper;
import operators.utils.CategoryViewsPairMapper;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple3;

import java.io.Serializable;

/**
 * Created by nickozoulis on 06/07/2016.
 */
public class BatchCategoryStdevViewsJob extends PookaBatchJob implements Serializable {
    private static final Logger logger = Logger.getLogger(BatchCategoryStdevViewsJob.class);
    private static final long serialVersionUID = -4160227974516109214L;

    public BatchCategoryStdevViewsJob(String appName, String mode, Long startTS, Long endTS) {
        super(appName, mode, new CategoryViewsMapper(), startTS, endTS);
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryViewsPairMapper());

        /*  Using aggregateByKey to perform StatCounter aggregation,
            so actually I can even have more statistics available */
        JavaPairRDD<String, StatCounter> stats = pairs.aggregateByKey(
                new StatCounter(),
                new MergeValue(),
                new MergeCombiner());

        return stats;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new ViewsToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        if (args.length != 2) System.exit(1);

        Long startTime = System.currentTimeMillis();

        Long startTS = Long.parseLong(args[0]);
        Long endTS = Long.parseLong(args[1]);

        new BatchCategoryStdevViewsJob("Avg", "local", startTS, endTS).start();

        Long endTime = System.currentTimeMillis();

        logger.info(Math.abs(endTime-startTime));
    }

}
