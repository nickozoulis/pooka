package operators.category_videos;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class CategoryPair implements PairFunction<String, String, Integer> {
    private static final long serialVersionUID = -5082925756081587982L;

    @Override
    public Tuple2<String, Integer> call(String s) throws Exception {
        return new Tuple2<>(s, 1);
    }
}
