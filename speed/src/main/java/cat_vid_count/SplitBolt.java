package cat_vid_count;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.io.Serializable;
import java.util.Map;

/**
 * Created by nickozoulis on 11/06/2016.
 */
public class SplitBolt extends BaseRichBolt implements Serializable {
    private static final long serialVersionUID = 8528960243602457880L;
    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    /**
     * Tuple format:
     * 0: video ID, an 11-digit string, which is unique
     * 1: uploader,	a string of the video uploader's username
     * 2: age, an integer number of days between the date when the video was uploaded and Feb.15, 2007 (YouTube's establishment)
     * 3: category,	a string of the video category chosen by the uploader
     * 4: length, an integer number of the video length
     * 5: views, an integer number of the views
     * 6: rate,	a float number of the video rate
     * 7: ratings, an integer number of the ratings
     * 8: comments,	an integer number of the comments
     * 9: related IDs, up to 20 strings of the related video IDs
     */
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\t");

        // Merge together all the related videos
        String relatedIds = "";
        for (int i=9; i<words.length; i++) {
            relatedIds += words[i] + " ";
        }

        try {
            collector.emit(new Values(
                    words[0],
                    words[1],
                    words[2],
                    words[3],
                    words[4],
                    words[5],
                    words[6],
                    words[7],
                    words[8],
                    relatedIds.trim()));
        } catch (Exception e) {e.printStackTrace();}

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(
                "videoId",
                "uploader",
                "age",
                "category",
                "length",
                "views",
                "rate",
                "ratings",
                "comments",
                "relatedIds"));
    }

}
