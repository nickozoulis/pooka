package coordinator;

import com.sun.management.OperatingSystemMXBean;
import coordinator.state.State;
import coordinator.state.StateAvgCatViews;
import coordinator.state.StateCountCategories;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by nickozoulis on 11/07/2016.
 */
public class CoordinatorAvgCatViews {
    private static final Logger logger = Logger.getLogger(CoordinatorAvgCatViews.class);

    public static void main(String[] args) throws IOException {


        if (args.length != 2) System.exit(1);

        while (true) {
            Long startTime = System.currentTimeMillis();

//        Long startTS = Long.parseLong(args[0]);
//        Long endTS = Long.parseLong(args[1]);

            Long minTS = Long.MAX_VALUE;
            Long maxTS = Long.MIN_VALUE;

            State state = new StateAvgCatViews();

            Configuration config = Utils.setHBaseConfig();

            // Instantiating HTable class
            HTable table = new HTable(config, Cons.MASTER_DATASET);

            // Instantiating the Scan class
            Scan scan = new Scan();

            // Scanning the required columns
            scan.addColumn(Bytes.toBytes(Cons.CF_MASTER_DATASET_INFO), Bytes.toBytes("category"));
            scan.addColumn(Bytes.toBytes(Cons.CF_MASTER_DATASET_INFO), Bytes.toBytes("views"));
//        scan.setTimeRange(startTS, endTS);

            // Getting the scan result
            ResultScanner scanner = null;

            try {
                scanner = table.getScanner(scan);

                // Reading values from scan result
                for (Result result = scanner.next(); result != null; result = scanner.next()) {
                    state.process(result);

                    Long ts = result.raw()[0].getTimestamp();
                    if (minTS > ts) {
                        minTS = ts;
                    }
                    if (maxTS < ts) {
                        maxTS = ts;
                    }
                }

            } finally {
                scanner.close();
                table.close();
            }

            Iterator iter = state.getState().entrySet().iterator();
            logger.info("----------------------------------------------");
            logger.info("firstTimestamp: " + minTS);
            logger.info("lastTimestamp: " + maxTS);
            while (iter.hasNext()) {
                Map.Entry pair = (Map.Entry) iter.next();

                logger.info(">>>>>>>>>>>> " + pair.getKey() + " : " + pair.getValue());
            }
            logger.info("----------------------------------------------");
            Long endTime = System.currentTimeMillis();

            logger.info("task duration: " + Math.abs(endTime - startTime));

            OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
                    .getOperatingSystemMXBean();

            logger.info("<> process_cpu_load: " + bean.getProcessCpuLoad());
            logger.info("<> process_cpu_time: " + bean.getProcessCpuTime());
            logger.info("<> system_cpu_load: " + bean.getSystemCpuLoad());
            logger.info("<> total_physical_memory_size: " + bean.getTotalPhysicalMemorySize());
            logger.info("<> fre_physical_memory_size: " + bean.getFreePhysicalMemorySize());

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
