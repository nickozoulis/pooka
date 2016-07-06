package cat_vid_count;

import org.apache.log4j.Logger;
import speed.storm.bolt.PookaView;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class ViewCount implements PookaView<String, Integer>, Serializable {
    private static final Logger logger = Logger.getLogger(ViewCount.class);
    private static final long serialVersionUID = -899854871055918800L;
    private Map<String, Integer> views;

    public ViewCount() {
        this.views = new HashMap<>();
    }

    public void process(String category) {
        if (views.containsKey(category)) {
            views.put(category, views.get(category) + 1);
            logger.info("Incremented category " + category);
            logger.info("Total count of category " + category + " is " + views.get(category));
        } else {
            views.put(category, 1);
            logger.info("First put of category " + category);
        }
    }

    @Override
    public Map<String, Integer> getView() {
        return views;
    }
}