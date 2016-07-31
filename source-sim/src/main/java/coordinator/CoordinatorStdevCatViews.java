package coordinator;

import coordinator.state.State;
import coordinator.state.StateStdevCatViews;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import serving.hbase.Utils;
import speed.storm.bolt.Cons;
import java.io.IOException;

/**
 * Created by nickozoulis on 11/07/2016.
 */
public class CoordinatorStdevCatViews {
    public static void main(String[] args) throws IOException {
        State state = new StateStdevCatViews();

        Configuration config = Utils.setHBaseConfig();
        config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        // Instantiating HTable class
        HTable table = new HTable(config, Cons.MASTER_DATASET);

        // Instantiating the Scan class
        Scan scan = new Scan();

        // Scanning the required columns
        scan.addColumn(Bytes.toBytes(Cons.CF_MASTER_DATASET_INFO), Bytes.toBytes("category"));
        scan.addColumn(Bytes.toBytes(Cons.CF_MASTER_DATASET_INFO), Bytes.toBytes("views"));

        // Getting the scan result
        ResultScanner scanner = null;

        try {
            scanner = table.getScanner(scan);

            // Reading values from scan result
            for (Result result = scanner.next(); result != null; result = scanner.next()) {
                state.process(result);
            }

        } finally {
            scanner.close();
        }
    }
}
