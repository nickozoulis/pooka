package storm.cat_stdev_views;

import org.apache.log4j.Logger;
import speed.storm.bolt.PookaView;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 04/07/2016.
 * <p>
 * Single pass (online) standard deviation algorithm.
 * Based on : https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance
 * <p>
 * def online_variance(data):
 * n = 0
 * mean = 0.0
 * M2 = 0.0
 * <p>
 * for x in data:
 * n += 1
 * delta = x - mean
 * mean += delta/n
 * M2 += delta*(x - mean)
 * <p>
 * if n < 2:
 * return float('nan')
 * else:
 * return M2 / (n - 1)
 */
public class ViewStdev implements PookaView<String, Double>, Serializable {
    private static final Logger logger = Logger.getLogger(ViewStdev.class);
    private static final long serialVersionUID = -1313477175020276377L;
    private Map<String, Double> mean;
    private Map<String, Double> M2;
    private Map<String, Integer> n;

    public ViewStdev() {
        this.mean = new HashMap<>();
        this.n = new HashMap<>();
        this.M2 = new HashMap<>();
    }

    public void process(String category, int views) {
        if (!n.containsKey(category)) {
            // Initializing data structures
            n.put(category, 1);
            mean.put(category, 0.0);
            M2.put(category, 0.0);
            logger.info("First put of category " + category);
        }

        // Instantiating local vars
        int n = this.n.get(category);
        double delta;
        double mean = this.mean.get(category);
        double m2 = this.M2.get(category);

        // Online stdev processing
        n++;
        delta = views - mean;
        mean += delta / n;
        m2 += delta * (views - mean);

        // Updating data structures
        this.n.put(category, n);
        this.mean.put(category, mean);
        this.M2.put(category, m2);
    }

    @Override
    public Map<String, Double> getView() {
        if (n.size() < 2) {
            throw new NumberFormatException("More than two tuples needed to compute a standard deviation");
        } else {
            Map<String, Double> stdev = new HashMap<>(n.size());
            int n;
            double m2;

            for (Map.Entry pair : this.n.entrySet()) {
                String cat = (String) pair.getKey();

                n = this.n.get(cat);
                m2 = this.M2.get(cat);

                stdev.put(cat, m2 / (n - 1));
            }

            return stdev;
        }
    }
}
