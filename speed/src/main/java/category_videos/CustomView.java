package category_videos;

import speed.storm.bolt.PookaView;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class CustomView implements PookaView<String, Integer>, Serializable {
    private static final long serialVersionUID = -899854871055918800L;
    private Map<String, Integer> views;

    public CustomView() {
        this.views = new HashMap<>();
    }

    public void process(String category) {
        if (views.containsKey(category)) {
            views.put(category, views.get(category) + 1);
        } else {
            views.put(category, 1);
        }
    }

    @Override
    public Map<String, Integer> getView() {
        return views;
    }
}
