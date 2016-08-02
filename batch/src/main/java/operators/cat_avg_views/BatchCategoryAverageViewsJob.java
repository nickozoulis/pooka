package operators.cat_avg_views;

import batch.spark.PookaBatchJob;
import operators.utils.CategoryViewsMapper;
import operators.utils.CategoryViewsPairMapper;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class BatchCategoryAverageViewsJob extends PookaBatchJob implements Serializable {
    private static final Logger logger = Logger.getLogger(BatchCategoryAverageViewsJob.class);
    private static final long serialVersionUID = 4478393882397153293L;

    public BatchCategoryAverageViewsJob(String appName, String mode, Long startTS, Long endTS) {
        super(appName, mode, new CategoryViewsMapper(), startTS, endTS);
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryViewsPairMapper());
        JavaPairRDD<String, ViewsAvg> counters = pairs.combineByKey(
                new CreateCombiner(),
                new MergeValue(),
                new MergeCombiner());

        return counters;
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

        new BatchCategoryAverageViewsJob("Avg", "local", startTS, endTS).start();

        Long endTime = System.currentTimeMillis();

        logger.info(Math.abs(endTime-startTime));
    }

}
