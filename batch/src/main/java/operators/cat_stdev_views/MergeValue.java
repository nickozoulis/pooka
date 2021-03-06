package operators.cat_stdev_views;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.util.StatCounter;

/**
 * Created by nickozoulis on 06/07/2016.
 */
public class MergeValue implements Function2<StatCounter, Integer, StatCounter> {

    private static final long serialVersionUID = 2490815237296397859L;

    @Override
    public StatCounter call(StatCounter v1, Integer v2) throws Exception {
       return v1.merge(v2);
    }
}
