package storm.cat_avg_views;

import org.apache.log4j.Logger;
import speed.storm.bolt.PookaView;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 02/07/2016.
 */
public class ViewAvg implements PookaView<String, Double>, Serializable {
    private static final Logger logger = Logger.getLogger(ViewAvg.class);
    private static final long serialVersionUID = -7771464715482042960L;
    private Map<String, Integer> views_sum;
    private Map<String, Integer> views_total;

    public ViewAvg() {
        this.views_sum = new HashMap<>();
        this.views_total = new HashMap<>();
    }

    public void process(String category, int views) {
        if (views_sum.containsKey(category)) {
            views_sum.put(category, views_sum.get(category) + views);
            views_total.put(category, views_total.get(category) + 1);
        } else {
            views_sum.put(category, views);
            views_total.put(category, 1);
            logger.info("First put of category " + category);
        }
    }

    @Override
    public Map<String, Double> getView() {
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
