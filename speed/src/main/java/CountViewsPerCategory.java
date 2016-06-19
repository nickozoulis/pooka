import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaTumblingWindowBolt;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by nickozoulis on 12/06/2016.
 */
public class CountViewsPerCategory extends PookaTumblingWindowBolt implements Serializable {
    private static final long serialVersionUID = -5450543934011469231L;
    private HashMap<String, Integer> counters;
    private Long windowId;
    private List<Put> rawPuts;

    @Override
    public void execute(TupleWindow inputWindow) {
        windowId = System.currentTimeMillis();
        rawPuts = new ArrayList<>();
        counters = new HashMap<>();

        String cat;
        for (Tuple tuple : inputWindow.get()) {
            addRawPut(tuple);
            cat = tuple.getStringByField("category");

            if (!counters.containsKey(cat)) {
                counters.put(cat, 1);
            } else {
                counters.put(cat, counters.get(cat) + 1);
            }
            getCollector().ack(tuple);
        }

        writeToHBase();
    }

    private void addRawPut(Tuple tuple) {
        try {
            Put p = new Put(toBytes(tuple.getStringByField("videoId")), windowId);

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

            rawPuts.add(p);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void writeToHBase() {
        try {
            writeSpeedViewToHBase();
            getTableRaw().put(rawPuts);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeSpeedViewToHBase() throws IOException {
        Put p = new Put(Bytes.toBytes(windowId), windowId);
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            p.addColumn(Cons.CF_VIEWS.getBytes(), toBytes(entry.getKey()), toBytes(entry.getValue()));
        }
        getTableSpeed().put(p);
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
