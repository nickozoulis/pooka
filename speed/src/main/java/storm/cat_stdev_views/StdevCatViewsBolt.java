package storm.cat_stdev_views;

import org.apache.hadoop.hbase.client.Put;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;

import java.io.Serializable;

/**
 * Created by nickozoulis on 04/07/2016.
 */
public class StdevCatViewsBolt extends PookaOutputBolt implements Serializable {
    private static final long serialVersionUID = 4456523762407437691L;

    public StdevCatViewsBolt(int numOfInputBolts) {
        super(numOfInputBolts, ViewStdev.class);
    }

    @Override
    protected void processTuple(Tuple input) {
        String category = input.getStringByField("category");
        int views = Integer.parseInt(input.getStringByField("views"));

        ((ViewStdev) getPookaBundle().getViewMap().get(getWindow()))
                .process(category, views);
    }

    @Override
    public Put createPutFromTuple(Tuple tuple) {
        Put p = new Put(toBytes(tuple.getStringByField("videoId")), getWindow());
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

    @Override
    public String queryPrefix() {
        return Cons.stdevPrefix;
    }
}
