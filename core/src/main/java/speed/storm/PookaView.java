package speed.storm;

import org.apache.hadoop.hbase.client.HTable;
import java.io.Serializable;

/**
 * Created by nickozoulis on 20/06/2016.
 */
public class PookaView implements Serializable {

    private static final long serialVersionUID = -847248015297507549L;
    private HTable tableSpeed, tableRaw;

    public PookaView(HTable tableSpeed, HTable tableRaw) {
        this.tableSpeed = tableSpeed;
        this.tableRaw = tableRaw;
    }

    public HTable getTableSpeed() {
        return tableSpeed;
    }

    public HTable getTableRaw() {
        return tableRaw;
    }

}
