package operators.cat_avg_views;

import batch.spark.PookaBatchJob;
import operators.utils.CategoryViewsMapper;
import operators.utils.CategoryViewsPairMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class BatchCategoryAverageViewsJob extends PookaBatchJob implements Serializable {
    private static final long serialVersionUID = 4478393882397153293L;

    public BatchCategoryAverageViewsJob(String appName, String mode) {
        super(appName, mode, new CategoryViewsMapper());
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
        new BatchCategoryAverageViewsJob("Avg", "local").start();
    }

}