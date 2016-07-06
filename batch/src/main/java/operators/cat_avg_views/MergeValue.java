package operators.cat_avg_views;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class MergeValue implements Function2<ViewsAvg, Integer, ViewsAvg> {
    private static final long serialVersionUID = -6402797157031878577L;

    @Override
    public ViewsAvg call(ViewsAvg v1, Integer v2) throws Exception {
       return new ViewsAvg(v1, v2);
    }
}
