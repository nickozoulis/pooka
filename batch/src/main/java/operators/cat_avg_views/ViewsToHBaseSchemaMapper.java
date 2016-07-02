package operators.cat_avg_views;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import speed.storm.bolt.Cons;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class ViewsToHBaseSchemaMapper implements PairFunction<Tuple2<String, Views>, ImmutableBytesWritable, Put> {
    private static final long serialVersionUID = -4730771052585092417L;
    private final Long batchTimestamp;

    public ViewsToHBaseSchemaMapper(Long batchTimestamp) {
        this.batchTimestamp = batchTimestamp;
    }

    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Views> pair) throws Exception {
        Put put = new Put(Bytes.toBytes(batchTimestamp), batchTimestamp);
        put.addColumn(Bytes.toBytes(Cons.CF_VIEWS),
                Bytes.toBytes("avg_" + pair._1()),
                Bytes.toBytes(pair._2().getAvg()+""));

        return new Tuple2<>(new ImmutableBytesWritable(), put);
    }
}
