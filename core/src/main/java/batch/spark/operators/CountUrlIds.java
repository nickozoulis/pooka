package batch.spark.operators;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class CountUrlIds implements Function2<Integer, Integer, Integer>  {
    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
    }
}
