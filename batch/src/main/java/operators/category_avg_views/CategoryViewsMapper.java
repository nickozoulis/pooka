package operators.category_avg_views;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import speed.storm.bolt.Cons;
import java.io.Serializable;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class CategoryViewsMapper implements Function<Tuple2<ImmutableBytesWritable, Result>, String>, Serializable {
    private static final long serialVersionUID = -6600196463553841440L;

    public String call(Tuple2<ImmutableBytesWritable, Result> tuple) throws Exception {
        String s;
        try {
            Result result = tuple._2;

            s = Bytes.toString(result.getValue(Cons.CF_MASTER_DATASET_INFO.getBytes(), "category".getBytes()));
            s += " ";
            s += Bytes.toString(result.getValue(Cons.CF_MASTER_DATASET_INFO.getBytes(), "views".getBytes()));

            return s;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}

