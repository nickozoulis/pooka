package operators.cat_stdev_views;

import batch.spark.PookaBatchJob;
import operators.utils.CategoryViewsMapper;
import operators.utils.CategoryViewsPairMapper;
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

    private static final long serialVersionUID = -4160227974516109214L;

    public BatchCategoryStdevViewsJob(String appName, String mode) {
        super(appName, mode, new CategoryViewsMapper());
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
        new BatchCategoryStdevViewsJob("Avg", "local").start();
    }

}
