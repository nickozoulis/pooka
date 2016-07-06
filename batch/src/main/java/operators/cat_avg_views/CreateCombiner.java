package operators.cat_avg_views;

import org.apache.spark.api.java.function.Function;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class CreateCombiner implements Function<Integer, ViewsAvg> {
    private static final long serialVersionUID = 6713044492567744884L;

    @Override
    public ViewsAvg call(Integer v1) throws Exception {
        return new ViewsAvg(v1);
    }
}
