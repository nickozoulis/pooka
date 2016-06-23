package operators.cat_avg_views;

import org.apache.spark.api.java.function.Function;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class CreateCombiner implements Function<Integer, Views> {
    private static final long serialVersionUID = 6713044492567744884L;

    @Override
    public Views call(Integer v1) throws Exception {
        return new Views(v1);
    }
}
