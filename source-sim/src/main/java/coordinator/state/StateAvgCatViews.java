package coordinator.state;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import speed.storm.bolt.Cons;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 11/07/2016.
 */
public class StateAvgCatViews implements State {
    private Map<String, Integer> views_sum;
    private Map<String, Integer> views_total;

    public StateAvgCatViews() {
        this.views_sum = new HashMap<>();
        this.views_total = new HashMap<>();
    }

    @Override
    public void process(Result result) {
        String cat = Bytes.toString(result.getValue(Cons.CF_MASTER_DATASET_INFO.getBytes(), "category".getBytes()));
        int views = Bytes.toInt(result.getValue(Cons.CF_MASTER_DATASET_INFO.getBytes(), "views".getBytes()));

        process(cat, views);
    }

    private void process(String category, int views) {
        if (views_sum.containsKey(category)) {
            views_sum.put(category, views_sum.get(category) + views);
            views_total.put(category, views_total.get(category) + 1);
        } else {
            views_sum.put(category, views);
            views_total.put(category, 1);
        }
    }

    @Override
    public Map getState() {
        Map<String, Double> avg = new HashMap<>(views_sum.size());

        for (Map.Entry pair : views_sum.entrySet()) {
            String cat = (String) pair.getKey();
            int sum = (int) pair.getValue();
            int total = views_total.get(cat);

            avg.put(cat, sum / (double) total);
        }

        return avg;
    }


}
