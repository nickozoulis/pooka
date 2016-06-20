import org.apache.storm.tuple.Tuple;
import speed.storm.bolt.PookaOutputBolt;
import java.io.Serializable;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class CountCategoryViewsBolt extends PookaOutputBolt implements Serializable {

    private Long timestamp;

    @Override
    public void execute(Tuple input) {
        timestamp = input.getLongByField("timestamp");

        String category;
        if (input.getBooleanByField("flag")) {
            category = input.getStringByField("category");

            if (!getViews().containsKey(timestamp)) {
                getViews().put(timestamp, new MyView(getTableSpeed(), getTableRaw()));
            }

            ((MyView) getViews().get(timestamp)).process(category);
        } else {
            getViews().get(timestamp).flush();
            getViews().remove(timestamp);
        }
    }

}
