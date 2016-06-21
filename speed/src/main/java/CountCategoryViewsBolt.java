import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class CountCategoryViewsBolt extends PookaOutputBolt implements Serializable {
    private static final long serialVersionUID = -3814974326725789289L;
    private Long timestamp;

    @Override
    public void execute(Tuple input) {
        timestamp = input.getLongByField("timestamp");

        String category;
        if (input.getBooleanByField("flag")) {
            category = input.getStringByField("category");

            if (!getViews().containsKey(timestamp)) {
                getViews().put(timestamp, new MyView(getTableSpeed(), getTableRaw()));
            }

            ((MyView) getViews().get(timestamp)).process(category);
            addRawPut(input);
        } else {
            flush(timestamp);
            getViews().remove(timestamp);
        }
    }

    private void flush(Long timestamp) {
        try {
            writeSpeedViewToHBase(timestamp);
            getTableRaw().put(getRawPuts().get(timestamp));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeSpeedViewToHBase(Long timestamp) throws IOException {
        Put p = new Put(Bytes.toBytes(timestamp), timestamp);
        for (Map.Entry<String, Integer> entry : ((MyView) getViews().get(timestamp)).getViews().entrySet()) {
            p.addColumn(Cons.CF_VIEWS.getBytes(), toBytes("count_" + entry.getKey()), toBytes(entry.getValue()));
        }
        getTableSpeed().put(p);
    }

    private void addRawPut(Tuple tuple) {
        try {
            Put p = new Put(toBytes(tuple.getStringByField("videoId")), timestamp);

            byte[] cf;
            byte[] value;
            for (String field : tuple.getFields()) {
                if (!field.equals("videoId")) {
                    cf = Cons.CF_MASTER_DATASET_INFO.getBytes();

                    if (field.equals("age") ||
                            field.equals("length") ||
                            field.equals("views") ||
                            field.equals("ratings") ||
                            field.equals("comments")) {
                        value = toBytes(Integer.parseInt(tuple.getStringByField((field))));
                    } else if (field.equals("category")) {
                        value = toBytes(tuple.getStringByField(field));
                    } else if (field.equals("rate")) {
                        value = toBytes(Double.parseDouble(tuple.getStringByField(field)));
                    } else { // uploader, relatedIds
                        cf = Cons.CF_MASTER_DATASET_OTHER.getBytes();
                        value = toBytes(tuple.getStringByField(field));
                    }

                    p.addColumn(cf, toBytes(field), value);
                }
            }

            getRawPuts().get(timestamp).add(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] toBytes(String b) {
        return Bytes.toBytes(b);
    }

    private byte[] toBytes(Integer b) {
        return Bytes.toBytes(b);
    }

    private byte[] toBytes(Double b) {
        return Bytes.toBytes(b);
    }

}
