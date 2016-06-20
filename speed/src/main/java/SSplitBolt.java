import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import speed.storm.bolt.PookaInputBolt;

import java.io.Serializable;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class SSplitBolt extends PookaInputBolt implements Serializable {
    private static final long serialVersionUID = 6110091644242967354L;

    @Override
    public void execute(TupleWindow inputWindow) {
        setTimestamp(System.currentTimeMillis());

        for (Tuple input : inputWindow.get()) {
            String sentence = input.getString(0);
            String[] words = sentence.split("\\t");

            // Merge together all the related videos
            String relatedIds = "";
            for (int i = 9; i < words.length; i++) {
                relatedIds += words[i] + " ";
            }


            getCollector().emit(new Values(
                    getTimestamp(),
                    words[0],
                    words[1],
                    words[2],
                    words[3],
                    words[4],
                    words[5],
                    words[6],
                    words[7],
                    words[8],
                    relatedIds.trim(),
                    true));
        }
        // Special msg indicate end of window
        getCollector().emit(new Values(
                getTimestamp(),
                "", "", 0, "", 0, 0, 0, "", "", false
        ));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "timestamp",
                "videoId",
                "uploader",
                "age",
                "category",
                "length",
                "views",
                "rate",
                "ratings",
                "comments",
                "relatedIds",
                "flag"));
    }
}
