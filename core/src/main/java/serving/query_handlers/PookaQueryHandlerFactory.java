package serving.query_handlers;

import serving.hbase.PookaQuery;

/**
 * Created by nickozoulis on 19/07/2016.
 */
public class PookaQueryHandlerFactory {

    public QueryHandler getHandler(PookaQuery query) {
        QueryHandler queryHandler = null;

        try {
            switch (query) {
                case COUNT_CATEGORY_VIEWS:
                    queryHandler = new CountH andler();
                    break;
                case AVG_CATEGORY_VIEWS:
                    queryHandler = new AvgHandler();
                    break;
                case STDEV_CATEGORY_VIEWS:
                    queryHandler = new StdevHandler();
                    break;
                default:
                    throw new QueryNotSupportedException("Not supported query: " + query);
            }
        } catch (QueryNotSupportedException e) {
            System.err.println(e.getMessage());
        }

        return queryHandler;
    }

}
