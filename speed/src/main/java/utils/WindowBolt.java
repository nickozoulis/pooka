package utils;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import speed.storm.bolt.PookaWindow;
import java.io.Serializable;

/**
 * Created by nickozoulis on 23/06/2016.
 */
public class WindowBolt extends PookaWindow implements Serializable {
    private static final long serialVersionUID = 3653733603889009274L;

    public WindowBolt(Long window) {
        super(window);
    }

    @Override
    public void execute(Tuple tuple) {
        getCollector().emit(new Values(
                getWindow(),
                tuple.getStringByField("videoId"),
                tuple.getStringByField("uploader"),
                tuple.getStringByField("age"),
                tuple.getStringByField("category"),
                tuple.getStringByField("length"),
                tuple.getStringByField("views"),
                tuple.getStringByField("views"),
                tuple.getStringByField("ratings"),
                tuple.getStringByField("comments"),
                tuple.getStringByField("relatedIds"),
                false));
    }

    @Override
    protected void emitAckTuple() {
        getCollector().emit(new Values(getWindow(), "", "", "", "", "", "", "", "", "", "", true));
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
