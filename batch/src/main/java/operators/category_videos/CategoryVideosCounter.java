package operators.category_videos;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class CategoryVideosCounter implements Function2<Integer, Integer, Integer>  {
    private static final long serialVersionUID = 2491300281714308212L;

    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
    }
}
