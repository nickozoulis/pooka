package operators.cat_stdev_views;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;

/**
 * Created by nickozoulis on 06/07/2016.
 */
public class MergeCombiner implements Function2<StatCounter, StatCounter, StatCounter> {

    private static final long serialVersionUID = 3039239868155156929L;

    @Override
    public StatCounter call(StatCounter v1, StatCounter v2) throws Exception {
        return v1.merge(v2);
    }
}
