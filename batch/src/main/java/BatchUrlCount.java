import batch.spark.PookaBatchJob;
import operators.category_views.CategoryMapper;
import operators.category_views.CategoryViewsCounter;
import operators.category_views.CategoryPair;
import operators.category_views.BatchResultToHBaseSchemaMapper;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchUrlCount extends PookaBatchJob implements Serializable {
    private static final long serialVersionUID = 7733556521224817301L;

    public BatchUrlCount(String appName, String mode) {
        super(appName, mode, new CategoryMapper());
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryPair());
        JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CategoryViewsCounter());

        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new BatchResultToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        new BatchUrlCount("Count", "local").start();
    }

}
