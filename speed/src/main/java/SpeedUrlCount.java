import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
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
public class SpeedUrlCount extends PookaTumblingWindowBolt implements Serializable {
    private static final long serialVersionUID = 1482487091002668338L;
    private HashSet<String> rawData;
    private HashMap<String, Integer> counters;
    private Long windowId;

    @Override
    public void execute(TupleWindow inputWindow) {
        rawData = new HashSet<>();
        counters = new HashMap<>();
        windowId = System.currentTimeMillis();

        for (Tuple tuple : inputWindow.get()) {
            rawData.add(tuple.getString(0));
            String str = tuple.getString(1);

            if (!counters.containsKey(str)) {
                counters.put(str, 1);
            } else {
                Integer c = counters.get(str) + 1;
                counters.put(str, c);
            }
            getCollector().ack(tuple);
        }

        writeToHBase();
    }

    private void writeToHBase() {
        try {
            writeRawToHBase();
            writeSpeedViewToHBase();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeSpeedViewToHBase() throws IOException {
        Put p = new Put(Bytes.toBytes(windowId), windowId);
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(">>>>>>>>> " + entry.getKey() + " " + entry.getValue());
            p.addColumn(Cons.CF_SPEED.getBytes(), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        getTableSpeed().put(p);
    }

    public void writeRawToHBase() throws IOException {
        Put p;
        for (String str : rawData) {
            System.out.println(">>>>>>>>> " + str);
            p = new Put(Bytes.toBytes(str), windowId);
            p.addColumn(Cons.CF_MASTER_DATASET.getBytes(),
                    Bytes.toBytes(""), Bytes.toBytes(""));
            getTableRaw().put(p);
        }
    }
}
