package operators.cat_avg_views;

import batch.spark.PookaBatchJob;
import com.sun.management.OperatingSystemMXBean;
import operators.cat_vid_count.BatchCategoryVideosJob;
import operators.utils.CategoryViewsMapper;
import operators.utils.CategoryViewsPairMapper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import java.io.Serializable;
import java.lang.management.ManagementFactory;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class BatchCategoryAverageViewsJob extends PookaBatchJob implements Serializable {
    private static final long serialVersionUID = 4478393882397153293L;

    public BatchCategoryAverageViewsJob(String appName, String mode) {
        super(appName, mode, new CategoryViewsMapper());
    }

    @Override
    public JavaPairRDD DAG() {
        JavaPairRDD<String, Integer> pairs = getBatchRDD().mapToPair(new CategoryViewsPairMapper());
        JavaPairRDD<String, ViewsAvg> counters = pairs.combineByKey(
                new CreateCombiner(),
                new MergeValue(),
                new MergeCombiner());

        return counters;
    }

    @Override
    public PairFunction hbaseSchemaAdapter() {
        return new ViewsToHBaseSchemaMapper(getBatchTimestamp());
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        while (true) {
            long start = System.currentTimeMillis();
            new BatchCategoryAverageViewsJob("Avg", "local").start();
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