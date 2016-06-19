package operators.category_avg_views;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class MergeValue implements Function2<Views, Integer, Views> {
    private static final long serialVersionUID = -6402797157031878577L;

    @Override
    public Views call(Views v1, Integer v2) throws Exception {
       return new Views(v1, v2);
    }
}
