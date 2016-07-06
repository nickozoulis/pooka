package operators.cat_stdev_views;

import org.apache.spark.api.java.function.Function;

/**
 * Created by nickozoulis on 06/07/2016.
 */
public class CreateCombiner implements Function<Integer, ViewsStdev> {

    private static final long serialVersionUID = 8061291504171273379L;

    @Override
    public ViewsStdev call(Integer v1) throws Exception {
        return new ViewsStdev(v1);
    }
}
