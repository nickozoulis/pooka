package cat_vid_count;

import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;
import java.io.Serializable;
import java.util.ArrayList;


/**
 * Created by nickozoulis on 20/06/2016.
 */
public class CountCategoryViewsBolt extends PookaOutputBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(CountCategoryViewsBolt.class);
    private static final long serialVersionUID = -1158550217238014753L;
    private Long window;

    public CountCategoryViewsBolt(int numOfInputBolts) {
        super(numOfInputBolts);
    }

    @Override
    public void execute(Tuple input) {
        window = input.getLongByField("window");

        String category;
        if (!input.getBooleanByField("ack")) {
            logger.info("Received normal tuple");
            category = input.getStringByField("category");

            if (!getPookaBundle().getViewMap().containsKey(window)) {
                logger.info("Initialising PookaBundle structures for window: " + window);
                getPookaBundle().getViewMap().put(window, new CustomView());
                getPookaBundle().getRawPuts().put(window, new ArrayList<Put>());
                getPookaBundle().getAcks().put(window, 0);
            }

            ((CustomView) getPookaBundle().getViewMap().get(window)).process(category);
            logger.info("Processed tuple for speed view");

            Put p = createPutFromTuple(input);
            getPookaBundle().getRawPuts().get(window).add(p);
            logger.info("Tuple appended to raw puts");
        } else {
            logger.info("Received ack tuple");
            // If all window bolts have sent their data, proceed to flush.
            if (getPookaBundle().processAck(window)) {
                logger.info("All " + getNumOfInputBolts() + " ack tuples gathered for window with ID: " + window);
                flush(window, Cons.countPrefix);
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

}