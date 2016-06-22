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
    private static final long serialVersionUID = -1158550217238014753L;
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

            if (!getPookaBundle().getViewMap().containsKey(window)) {
                getPookaBundle().getViewMap().put(window, new MyView());
                getPookaBundle().getRawPuts().put(window, new ArrayList<Put>());
            }

            ((MyView) getPookaBundle().getViewMap().get(window)).process(category);

            Put p = createPutFromTuple(input);
            getPookaBundle().getRawPuts().get(window).add(p);
        } else {
            // If all window bolts sent their data, proceed to flush.
            if (getPookaBundle().processAck(window)) {
                flush(window);
            }
        }
    }

    private Put createPutFromTuple(Tuple tuple) {
        Put p = new Put(toBytes(tuple.getStringByField("videoId")), window);
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
        return p;
    }

    private void flush(Long window) {
        try {
            // Write raw data to master dataset in HBase.
            getTableRaw().put(getPookaBundle().getRawPuts().get(window));
            // Write speed views to speed view table in HBase.
            getTableSpeed().put(createPutFromView(window));
            // Remove data from bundle to release memory
            getPookaBundle().removeFromBundle(window);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Put createPutFromView(Long window) throws IOException {
        Put p = new Put(Bytes.toBytes(window), window);

        Map<String, Integer> m = getPookaBundle().getViewMap().get(window).getView();
        for (Map.Entry<String, Integer> entry : m.entrySet()) {
            p.addColumn(Cons.CF_VIEWS.getBytes(), toBytes("count_" + entry.getKey()), toBytes(entry.getValue()));
        }
        return p;
    }

}
