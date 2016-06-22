import org.apache.hadoop.hbase.client.HTable;
import speed.storm.PookaView;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class MyView extends PookaView implements Serializable {

    private static final long serialVersionUID = -8448977864355211360L;
    private Map<String, Integer> views;


    public MyView(HTable tableSpeed, HTable tableRaw) {
        super(tableSpeed, tableRaw);
        this.views = new HashMap<>();
    }

    public void process(String category) {
        if (views.containsKey(category)) {
            views.put(category, views.get(category) + 1);
        } else {
            views.put(category, 1);
        }
    }

    public Map<String, Integer> getViews() {
        return views;
    }
}