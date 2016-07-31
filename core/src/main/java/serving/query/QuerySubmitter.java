package serving.query;

import serving.hbase.PookaQuery;
import java.io.IOException;

/**
 * Created by nickozoulis on 31/07/2016.
 */
public class QuerySubmitter {
    public static void submit(String speed, String batch, PookaQuery query) {

        switch (speed) {
            case "storm":
                submitStormTopology(query);
                break;
            default:
                break;
        }

        switch (batch) {
            case "spark":
                submitSparkJob();
                break;
            default:
                break;
        }
    }

    private static void submitSparkJob() {
    }

    private static void submitStormTopology(PookaQuery query) {
        String queryMain = "";

        switch (query) {
            case COUNT_CATEGORY_VIEWS:
                queryMain = "storm.cat_vid_count.Main";
                break;
            case AVG_CATEGORY_VIEWS:
                queryMain = "storm.cat_avg_views.Main";
                break;
            case STDEV_CATEGORY_VIEWS:
                queryMain = "storm.cat_stdev_views.Main";
                break;
            default:
                try {
                    throw new QueryNotSupportedException();
                } catch (QueryNotSupportedException e) {
                    e.printStackTrace();
                }
        }

        try {
            new ProcessBuilder("java -cp speed-1.0-SNAPSHOT-all.jar " + queryMain).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
