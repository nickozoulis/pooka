package operators.category_avg_views;


import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class MergeCombiner implements Function2<Views, Views, Views> {
    private static final long serialVersionUID = 928230628845756295L;

    @Override
    public Views call(Views v1, Views v2) throws Exception {
        return new Views(v1, v2);
    }
}
