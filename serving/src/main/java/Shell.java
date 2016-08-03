import jline.console.ConsoleReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import serving.hbase.PookaQuery;
import serving.hbase.Utils;
import jline.TerminalFactory;
import serving.query.QueryHandler;

import java.io.IOException;
import java.util.HashSet;

/**
 * Created by nickozoulis on 18/06/2016.
 */
public class Shell {
    private static final Logger logger = Logger.getLogger(Shell.class);
    private static HashSet<PookaQuery> pookaQueries = new HashSet<>();
    private static ConsoleReader console;

    public Shell() {
        try {
            console = new ConsoleReader();
            console.setPrompt("pooka> ");

            String line;
            // Gets user's input
            while ((line = console.readLine()) != null) {
                parse(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // Does some cleanup and restores the original terminal configuration.
            try {
                TerminalFactory.get().restore();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void parse(String line) {
        switch (line) {
            case "count":
                logger.info("Issuing count query...");
                handleQuery(PookaQuery.COUNT_CATEGORY_VIEWS);
                break;
            case "avg":
                logger.info("Issuing avg query...");
                handleQuery(PookaQuery.AVG_CATEGORY_VIEWS);
                break;
            case "stdev":
                logger.info("Issuing stdev query...");
                handleQuery(PookaQuery.STDEV_CATEGORY_VIEWS);
                break;
            default:
                break;
        }
    }

    /**
     * Handles incoming query in parallel by starting new thread.
     * @param query The type of the query to be issued.
     */
    //TODO: Use Thread Pooling for efficiency
    private static void handleQuery(PookaQuery query) {
        QueryHandler queryHandler;

        if (pookaQueries.contains(query)) {
            queryHandler = new QueryHandler(query, true);
        } else {
            pookaQueries.add(query);
            queryHandler = new QueryHandler(query, false);
        }

        new Thread(queryHandler).start();
    }

    public static void main(String[] args) {
        new Shell();
    }

}
