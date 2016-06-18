import batch.spark.PookaBatchJob;
import operators.CountUrlIds;
import operators.ExtractUrlIds;
import operators.MapToHBaseSchema;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchUrlCount extends PookaBatchJob implements Serializable {
    private static final long serialVersionUID = 7733556521224817301L;

    public BatchUrlCount(String appName, String mode) {
        super(appName, mode);
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new ExtractUrlIds());
        JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CountUrlIds());

        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new MapToHBaseSchema(getBatchTimestamp());
    }

    public static void main(String[] args) {
        new BatchUrlCount("Count", "local").start();
    }

}
