package coordinator.state;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import speed.storm.bolt.Cons;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 11/07/2016.
 */
public class StateCountCategories implements State {
    private Map<String, Integer> catViews;

    public StateCountCategories() {
        this.catViews = new HashMap<>();
    }

    @Override
    public void process(Result result) {
        String cat = Bytes.toString(result.getValue(Cons.CF_MASTER_DATASET_INFO.getBytes(), "category".getBytes()));

        process(cat);
    }

    private void process(String category) {
        if (catViews.containsKey(category)) {
            catViews.put(category, catViews.get(category) + 1);
        } else {
            catViews.put(category, 1);
        }
    }

    @Override
    public Map getState() {
        return catViews;
    }


}
