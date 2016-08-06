package serving.query;

import org.apache.log4j.Logger;
import serving.hbase.PookaQuery;
import java.io.IOException;

/**
 * Created by nickozoulis on 31/07/2016.
 */
public class QuerySubmitter {
    private static final Logger logger = Logger.getLogger(QuerySubmitter.class);
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
                submitSparkJob(query);
                break;
            default:
                break;
        }
    }

    private static void submitSparkJob(PookaQuery query) {
        String queryMain = "operators.";

        switch (query) {
            case COUNT_CATEGORY_VIEWS:
                queryMain += ".cat_vid_count.BatchCategoryVideosJob";
                break;
            case AVG_CATEGORY_VIEWS:
                queryMain += "cat_avg_views.BatchCategoryAverageViewsJob";
                break;
            case STDEV_CATEGORY_VIEWS:
                queryMain += "cat_stdev_views.BatchCategoryStdevViewsJob";
                break;
            default:
                try {
                    throw new QueryNotSupportedException();
                } catch (QueryNotSupportedException e) {
                    e.printStackTrace();
                }
        }

        try {
            logger.info("Submitting query: " + query.toString());
            Runtime.getRuntime().exec("java -cp /home/nickoszoulis/batch-1.0-SNAPSHOT-all.jar " + queryMain);
//            new ProcessBuilder("java -cp /home/nickoszoulis/batch-1.0-SNAPSHOT-all.jar " + queryMain).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
            logger.info("Submitting query: " + query.toString());
            Runtime.getRuntime().exec("java -cp /home/nickoszoulis/speed-all-1.0-SNAPSHOT.jar " + queryMain);
//            new ProcessBuilder("java -cp  /home/nickoszoulis/speed-all-1.0-SNAPSHOT.jar " + queryMain).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
