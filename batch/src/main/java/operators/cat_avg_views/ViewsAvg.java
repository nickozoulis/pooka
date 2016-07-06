package operators.cat_avg_views;

import java.io.Serializable;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class ViewsAvg implements Serializable {
    private static final long serialVersionUID = -2352703868507808352L;
    private int count;
    private int value;

    public ViewsAvg(int value) {
        setCount(1);
        setValue(value);
    }

    public ViewsAvg(ViewsAvg v, int value) {
        this.count = v.getCount() + 1;
        this.value = v.getValue() + value;
    }

    public ViewsAvg(ViewsAvg v1, ViewsAvg v2) {
        this.count = v1.getCount() + v2.getCount();
        this.value = v1.getValue() + v2.getValue();
    }

    public double getAvg() {
        return (double)this.value / this.count;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public int getValue() {
        return value;
    }

    public void setValue(int value) {
        this.value = value;
    }
}
