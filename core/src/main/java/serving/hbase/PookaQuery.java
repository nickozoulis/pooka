package serving.hbase;

/**
 * Created by nickozoulis on 18/07/2016.
 */
public enum PookaQuery {
    COUNT_CATEGORY_VIEWS, AVG_CATEGORY_VIEWS, STDEV_CATEGORY_VIEWS;

    @Override
    public String toString() {
        switch (this) {
            case COUNT_CATEGORY_VIEWS: return "count_";
            case AVG_CATEGORY_VIEWS: return "avg_";
            case STDEV_CATEGORY_VIEWS: return "stdev_";
            default: throw new IllegalArgumentException();
        }
    }
}
