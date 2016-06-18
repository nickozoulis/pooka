package operators;

import org.apache.spark.api.java.function.Function2;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class CountUrlIds implements Function2<Integer, Integer, Integer>  {
    private static final long serialVersionUID = -4928430253379230031L;

    @Override
    public Integer call(Integer v1, Integer v2) throws Exception {
        return v1 + v2;
    }
}
