package category_videos;

import org.apache.log4j.Logger;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;
import serving.hbase.Utils;
import speed.storm.bolt.PookaInputBolt;

import java.io.Serializable;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class SplitBolt extends PookaInputBolt implements Serializable {
    private static final Logger logger = Logger.getLogger(SplitBolt.class);
    private static final long serialVersionUID = -9056231194989007924L;

    public SplitBolt(Long window) {
        super(window);
    }

    @Override
    public void execute(Tuple input) {
        if (isTickTuple(input)) {
            // Special msg indicate end of window
            logger.info("Emitting ack tuple for window: " + getWindow());
            getCollector().emit(new Values(getWindow(), "", "", 0, "", 0, 0, 0, 0, "", "", true));
            incrementWindow();
        } else {
            String sentence = input.getString(0);
            String[] words = sentence.split("\\t");

            // Merge together all the related videos
            String relatedIds = "";
            for (int i = 9; i < words.length; i++) {
                relatedIds += words[i] + " ";
            }

            getCollector().emit(new Values(
                    getWindow(),
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
                    false));
            logger.info("Emitting tuple: " + words[3] + " for window " + getWindow());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "window",
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
                "ack"));
    }
}
