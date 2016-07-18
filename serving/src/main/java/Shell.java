import jline.console.ConsoleReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import serving.hbase.PookaQuery;
import serving.hbase.Utils;
import jline.TerminalFactory;
import serving.query_handlers.PookaQueryHandlerFactory;
import serving.query_handlers.QueryHandler;
import java.io.IOException;

/**
 * Created by nickozoulis on 18/06/2016.
 */
public class Shell {
    private static final Logger logger = Logger.getLogger(Shell.class);
    private static ConsoleReader console;
    private Configuration config;

    public static void main(String[] args) {
        new Shell();
    }

    public Shell() {
        config = Utils.setHBaseConfig();

        try {
            console = new ConsoleReader();
            console.setPrompt("serving_layer> ");

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

    private static void handleQuery(PookaQuery query) {
        PookaQueryHandlerFactory factory = new PookaQueryHandlerFactory();

        QueryHandler queryHandler = factory.getHandler(query);

        queryHandler.handle();
    }
}
