package cat_vid_count;

import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;
import java.io.Serializable;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class CountCategoryViewsBolt extends PookaOutputBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(CountCategoryViewsBolt.class);
    private static final long serialVersionUID = -1158550217238014753L;

    public CountCategoryViewsBolt(int numOfInputBolts) {
        super(numOfInputBolts, CustomViewCount.class);
    }

    @Override
    protected void processTuple(Tuple input) {
        String category = input.getStringByField("category");

        ((CustomViewCount) getPookaBundle().getViewMap().get(getWindow()))
                .process(category);
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
        return Cons.countPrefix;
    }

}