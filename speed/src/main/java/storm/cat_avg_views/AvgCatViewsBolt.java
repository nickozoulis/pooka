package storm.cat_avg_views;

import org.apache.hadoop.hbase.client.Put;
import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.Cons;
import speed.storm.bolt.PookaOutputBolt;
import java.io.Serializable;

/**
 * Created by nickozoulis on 02/07/2016.
 */
public class AvgCatViewsBolt extends PookaOutputBolt implements Serializable {
    private static final long serialVersionUID = -6202820600520815730L;

    public AvgCatViewsBolt(int numOfInputBolts) {
        super(numOfInputBolts, ViewAvg.class);
    }

    @Override
    protected void processTuple(Tuple input) {
        String category = input.getStringByField("category");
        int views = Integer.parseInt(input.getStringByField("views"));

        ((ViewAvg) getPookaBundle().getViewMap().get(getWindow()))
                .process(category, views);
    }

    @Override
    public Put createPutFromTuple(Tuple tuple) {
//        Put p = new Put(toBytes(tuple.getStringByField("videoId")), getWindow());
        Put p = new Put(toBytes(randomAlphaNumeric(11)), getWindow());
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
        return Cons.avgPrefix;
    }

    //TODO: Remove below as they exist in DatasetGenerator
    public static String randomAlphaNumeric(int count) {
        StringBuilder builder = new StringBuilder();
        int character;
        while (count-- != 0) {
            character = (int) (Math.random() * ALPHA_NUMERIC_STRING.length());
            builder.append(ALPHA_NUMERIC_STRING.charAt(character));
        }
        return builder.toString();
    }
    private static final String ALPHA_NUMERIC_STRING = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";

}
