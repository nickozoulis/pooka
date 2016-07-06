package operators.cat_stdev_views;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.util.StatCounter;
import scala.Tuple2;
import speed.storm.bolt.Cons;

/**
 * Created by nickozoulis on 06/07/2016.
 */
public class ViewsToHBaseSchemaMapper implements PairFunction<Tuple2<String, StatCounter>, ImmutableBytesWritable, Put> {
    private static final long serialVersionUID = -2334079384660328944L;
    private final Long batchTimestamp;

    public ViewsToHBaseSchemaMapper(Long batchTimestamp) {
        this.batchTimestamp = batchTimestamp;
    }

    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, StatCounter> pair) throws Exception {
        Put put = new Put(Bytes.toBytes(batchTimestamp), batchTimestamp);
        put.addColumn(Bytes.toBytes(Cons.CF_VIEWS),
                Bytes.toBytes("stdev_" + pair._1()),
                Bytes.toBytes(pair._2().stdev()+""));

        return new Tuple2<>(new ImmutableBytesWritable(), put);
    }
}
