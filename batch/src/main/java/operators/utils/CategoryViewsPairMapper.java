package operators.utils;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class CategoryViewsPairMapper implements PairFunction<String, String, Integer> {
    private static final long serialVersionUID = -1717700874499195380L;

    @Override
    public Tuple2<String, Integer> call(String s) throws Exception {
        String[] splits = s.split("---");
        return new Tuple2<>(splits[0], Integer.parseInt(splits[1]));
    }
}
