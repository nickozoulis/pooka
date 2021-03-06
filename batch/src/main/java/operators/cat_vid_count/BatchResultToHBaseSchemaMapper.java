package operators.cat_vid_count;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import speed.storm.bolt.Cons;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchResultToHBaseSchemaMapper implements PairFunction<Tuple2<String, Integer>, ImmutableBytesWritable, Put> {
    private static final long serialVersionUID = -3329516416693204721L;
    private final Long batchTimestamp;

    public BatchResultToHBaseSchemaMapper(Long batchTimestamp) {
        this.batchTimestamp = batchTimestamp;
    }

    @Override
    public Tuple2<ImmutableBytesWritable, Put> call(Tuple2<String, Integer> pair) throws Exception {
        Put put = new Put(Bytes.toBytes(batchTimestamp), batchTimestamp);
        put.addColumn(Bytes.toBytes(Cons.CF_VIEWS), Bytes.toBytes(pair._1()), Bytes.toBytes(pair._2()));
        return new Tuple2<>(new ImmutableBytesWritable(), put);
    }
}
