package operators.cat_avg_views;


import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class MergeCombiner implements Function2<ViewsAvg, ViewsAvg, ViewsAvg> {
    private static final long serialVersionUID = 928230628845756295L;

    @Override
    public ViewsAvg call(ViewsAvg v1, ViewsAvg v2) throws Exception {
        return new ViewsAvg(v1, v2);
    }
}
