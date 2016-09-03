package serving.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import serving.hbase.PookaQuery;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by nickozoulis on 19/07/2016.
 */
public class QueryHandler implements Runnable {
    private static final Logger logger = Logger.getLogger(QueryHandler.class);
    private Long batchLastTimestamp = 0l;
    private final boolean queryStatus;
    private final PookaQuery query;
    private static Configuration hBaseConfig = Utils.setHBaseConfig();
    private HTable tableSpeed, tableBatch;
    private Filter filter;
    static AtomicInteger COUNTER = new AtomicInteger(0);
    private Long speedLastTimestamp = 0l;

    public QueryHandler(PookaQuery query, boolean queryStatus) {
        this.query = query;
        this.queryStatus = queryStatus;

        filter = new ColumnPrefixFilter(Bytes.toBytes(query.toString()));

        try {
            tableSpeed = new HTable(hBaseConfig, Cons.TABLE_SPEED);
            tableBatch = new HTable(hBaseConfig, Cons.TABLE_BATCH);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Map<String, Double> gatherResult() {
        Map<String, Double> batch_view = gatherViewFromBatchTable();
        Map<String, Double> speed_view = gatherViewsFromSpeedTable();

        return mergeViews(batch_view, speed_view);
    }

    /**
     * Scan speed_view starting from the timestamp batch_view has ended.
     * @return
     */
    private Map<String, Double> gatherViewsFromSpeedTable() {
        logger.info("Gathering view(s) from speed table..");
        Map<String, Double> view = new HashMap<>();

        Scan scan = new Scan(Bytes.toBytes(batchLastTimestamp + 1));
        scan.addFamily(Cons.CF_VIEWS.getBytes());
        scan.setFilter(filter);

        try {
            ResultScanner rs = tableSpeed.getScanner(scan);

            for (Result r : rs) {
                view = mergeViews(view, getView(r));
                //TODO: Remove below line after experiments
                speedLastTimestamp = r.raw()[0].getTimestamp();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return view;
    }

    private Map<String, Double> mergeViews(Map<String, Double> viewOrig, Map<String, Double> viewNew) {
        Map m = new HashMap();
        switch (query) {
            case COUNT_CATEGORY_VIEWS:
                m = mergeCountViews(viewOrig, viewNew);
                break;
            case AVG_CATEGORY_VIEWS:
                m = mergeAvgViews(viewOrig, viewNew);
                break;
            case STDEV_CATEGORY_VIEWS:
                m = mergeStdevViews(viewOrig, viewNew);
                break;
            default:
                try {
                    throw new QueryNotSupportedException();
                } catch (QueryNotSupportedException e) {
                    e.printStackTrace();
                }
        }
        return m;
    }

    private Map mergeStdevViews(Map<String, Double> viewOrig, Map<String, Double> viewNew) {
        Iterator it = viewNew.entrySet().iterator();
        Map.Entry<String, Double> pair;

        while (it.hasNext()) {
            pair = (Map.Entry) it.next();

            String keyNew = pair.getKey();
            double valueNew = pair.getValue();

            if (viewOrig.containsKey(keyNew)) {
                viewOrig.put(keyNew, Math.sqrt(Math.pow(viewOrig.get(keyNew), 2) + Math.pow(valueNew, 2)));
            } else {
                viewOrig.put(keyNew, valueNew);
            }
        }

        return viewOrig;
    }

    private Map mergeAvgViews(Map<String, Double> viewOrig, Map<String, Double> viewNew) {
        Iterator it = viewNew.entrySet().iterator();
        Map.Entry<String, Double> pair;

        while (it.hasNext()) {
            pair = (Map.Entry) it.next();

            String keyNew = pair.getKey();
            double valueNew = pair.getValue();

            if (viewOrig.containsKey(keyNew)) {
                viewOrig.put(keyNew, (viewOrig.get(keyNew) + valueNew) / 2);
            } else {
                viewOrig.put(keyNew, valueNew);
            }
        }

        return viewOrig;
    }

    private Map<String, Double> mergeCountViews(Map<String, Double> viewOrig, Map<String, Double> viewNew) {
        Iterator it = viewNew.entrySet().iterator();
        Map.Entry<String, Double> pair;

        while (it.hasNext()) {
            pair = (Map.Entry) it.next();

            String keyNew = pair.getKey();
            double valueNew = pair.getValue();

            if (viewOrig.containsKey(keyNew)) {
                viewOrig.put(keyNew, viewOrig.get(keyNew) + valueNew);
            } else {
                viewOrig.put(keyNew, valueNew);
            }
        }

        return viewOrig;
    }

    /**
     * Reverse scan the batch view and get the last view of that query.
     * @return
     */
    private Map<String, Double> gatherViewFromBatchTable() {
        logger.info("Gathering view from batch table..");
        Map<String, Double> view = new HashMap<>();

        Scan scan = new Scan();
        scan.addFamily(Cons.CF_VIEWS.getBytes());
        scan.setReversed(true);
        scan.setFilter(filter);

        try {
            ResultScanner rs = tableBatch.getScanner(scan);
            for (Result r : rs) {
                view = getView(r);

                // Note the timestamp. To be used as starting point for speed_views scanning.
                batchLastTimestamp = new Long(r.raw()[0].getTimestamp());
                break;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return view;
    }

    private Map<String, Double> getView(Result r) {
        Map<String, Double> view = new HashMap<>();
        NavigableMap<byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes(Cons.CF_VIEWS));

        for (byte[] bQuantifer : familyMap.keySet()) {
            String key = Bytes.toString(bQuantifer);
            double value = Double.parseDouble(Bytes.toString(familyMap.get(bQuantifer)));

            view.put(key, value);
        }

        return view;
    }

    public synchronized void printResult(Map map, PookaQuery query) {
        logger.info("----------------------------------------------");
        logger.info("batchLastTimestamp: " + batchLastTimestamp);
        logger.info("speedLastTimestamp: " + speedLastTimestamp);
        logger.info(">>> Results of query: " + query.name());

        Iterator it = map.entrySet().iterator();
        Map.Entry pair;
        while (it.hasNext()) {
            pair = (Map.Entry) it.next();
            logger.info(">>>>>>>>>>>>" + pair.getKey() + " : " + pair.getValue());
        }
        logger.info("----------------------------------------------");
    }


    private void pollSpeedViewsTableForResult() {
        Map map;
        while (true) {
            System.out.println("Polling speed table for result..");
            map = gatherViewsFromSpeedTable();

            if (map.size() > 0) {
                printResult(map, query);
                break;
            }
        }
    }

    @Override
    public void run() {
        Long start = System.currentTimeMillis();
        if (queryStatus) {
            logger.info("Query " + query.toString() + " already exists, gathering views..");
            // Gather views from batch_views and speed_views tables.
            printResult(gatherResult(), query);
        } else {
            logger.info("First occurence of query " + query.toString());
            // Submit query in both speed and batch layer.
//            QuerySubmitter.submit("storm", "spark", query);
            // And start polling hbase for results.
            pollSpeedViewsTableForResult();
        }
        Long end = System.currentTimeMillis();
        logger.info("--> [query: " + query.toString() + ", id: " + COUNTER.getAndIncrement() + ", time: " + (end - start) +" ms] <--");


        OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
                .getOperatingSystemMXBean();

        logger.info("<> process_cpu_load: " + bean.getProcessCpuLoad());
        logger.info("<> process_cpu_time: " + bean.getProcessCpuTime());
        logger.info("<> system_cpu_load: " + bean.getSystemCpuLoad());
        logger.info("<> total_physical_memory_size: " + bean.getTotalPhysicalMemorySize());
        logger.info("<> fre_physical_memory_size: " + bean.getFreePhysicalMemorySize());
    }

}
