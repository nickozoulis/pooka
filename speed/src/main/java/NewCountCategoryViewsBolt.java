import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class NewCountCategoryViewsBolt extends PookaOutputBolt implements Serializable {
    private static final long serialVersionUID = -3814974326725789289L;
    private Long window;

    public NewCountCategoryViewsBolt(int numOfInputBolts) {
        super(numOfInputBolts);
    }

    @Override
    public void execute(Tuple input) {
        window = input.getLongByField("window");

        String category;
        if (!input.getBooleanByField("ack")) {
            category = input.getStringByField("category");

            if (!getViews().containsKey(window)) {
                getViews().put(window, new MyView(getTableSpeed(), getTableRaw()));
                getRawPuts().put(window, new ArrayList<Put>());
            }

            ((MyView) getViews().get(window)).process(category);
            addRawPut(input);
        } else {
            processAck(window);
        }
    }

    private void processAck(Long window) {
        getPookaBundle().processAck(window);
    }

    private void flush(Long timestamp) {
        try {
            getTableRaw().put(getRawPuts().get(timestamp));
            writeSpeedViewToHBase(timestamp);
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
            Put p = new Put(toBytes(tuple.getStringByField("videoId")), window);

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
                    } else if (field.equals("ack") || field.equals("window")) {
                        continue;
                    } else { // uploader, relatedIds
                        cf = Cons.CF_MASTER_DATASET_OTHER.getBytes();
                        value = toBytes(tuple.getStringByField(field));
                    }

                    p.addColumn(cf, toBytes(field), value);
                }
            }

            getRawPuts().get(window).add(p);
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
