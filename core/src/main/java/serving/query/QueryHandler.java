package serving.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import serving.hbase.PookaQuery;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;

/**
 * Created by nickozoulis on 19/07/2016.
 */
public class QueryHandler implements Runnable {
    private Long batchLastTimestamp = 0l;
    private final boolean queryStatus;
    private final PookaQuery query;
    private static Configuration hBaseConfig = Utils.setHBaseConfig();
    private HTable tableSpeed, tableBatch;
    private Filter filter;

    public QueryHandler(PookaQuery query, boolean queryStatus) {
        this.query = query;
        this.queryStatus = queryStatus;

        filter = new PrefixFilter(Bytes.toBytes(query.toString()));

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
        Map<String, Double> view = new HashMap<>();

        Scan scan = new Scan(Bytes.toBytes(batchLastTimestamp + 1));
        scan.addFamily(Cons.CF_VIEWS.getBytes());
        scan.setFilter(filter);

        try {
            ResultScanner rs = tableSpeed.getScanner(scan);

            for (Result r : rs) {
                view = mergeViews(view, getView(r));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return view;
    }

    private Map<String, Double> mergeViews(Map<String, Double> viewOrig, Map<String, Double> viewNew) {
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
        System.out.println("----------------------------------------------");
        System.out.println(">>> Results of query: " + query.name());

        Iterator it = map.entrySet().iterator();
        Map.Entry pair;
        while (it.hasNext()) {
            pair = (Map.Entry) it.next();
            System.out.println(pair.getKey() + " " + pair.getValue());
        }
        System.out.println("----------------------------------------------");
    }


    private void pollSpeedViewsTableForResult() {
        Map map;
        while (true) {
            map = gatherViewsFromSpeedTable();

            if (map.size() > 0) {
                printResult(map, query);
                break;
            }
        }
    }

    @Override
    public void run() {
        if (queryStatus) {
            // Gather views from batch_views and speed_views tables.
            printResult(gatherResult(), query);
        } else {
            // Submit query in both speed and batch layer.
            QuerySubmitter.submit("storm", "spark", query);
            // And start polling hbase for results.
            pollSpeedViewsTableForResult();
        }
    }

}
