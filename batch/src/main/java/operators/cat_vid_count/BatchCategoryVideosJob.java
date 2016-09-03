package operators.cat_vid_count;

import batch.spark.PookaBatchJob;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.lang.management.ManagementFactory;
import com.sun.management.OperatingSystemMXBean;

import java.io.Serializable;

/**
 * Created by nickozoulis on 17/06/2016.
 */
public class BatchCategoryVideosJob extends PookaBatchJob implements Serializable {
    private static final long serialVersionUID = 3420047706448356615L;

    public BatchCategoryVideosJob(String appName, String mode) {
        super(appName, mode, new CategoryMapper());
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryPair());
        JavaPairRDD<String, Integer> counters = pairs.reduceByKey(new CategoryVideosCounter());

        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new BatchResultToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        while (true) {
            long start = System.currentTimeMillis();
            new BatchCategoryVideosJob("Count", "local").start();
            long end = System.currentTimeMillis();
            System.out.println(">>> " + (end - start));

            OperatingSystemMXBean bean = (com.sun.management.OperatingSystemMXBean) ManagementFactory
                    .getOperatingSystemMXBean();

            System.out.println("<> process_cpu_load: " + bean.getProcessCpuLoad());
            System.out.println("<> process_cpu_time: " + bean.getProcessCpuTime());
            System.out.println("<> system_cpu_load: " + bean.getSystemCpuLoad());
            System.out.println("<> total_physical_memory_size: " + bean.getTotalPhysicalMemorySize());
            System.out.println("<> fre_physical_memory_size: " + bean.getFreePhysicalMemorySize());
        }
    }

}