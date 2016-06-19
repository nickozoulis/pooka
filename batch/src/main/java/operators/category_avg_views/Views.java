package operators.category_avg_views;

import java.io.Serializable;

/**
 * Created by nickozoulis on 19/06/2016.
 */
public class Views implements Serializable {
    private static final long serialVersionUID = -2352703868507808352L;
    private int count;
    private int value;

    public Views(int value){
        setCount(1);
        setValue(value);
    }

    public Views(Views v, int value) {
        this.count = v.getCount() + 1;
        this.value = v.getValue() + value;
    }

    public Views(Views v1, Views v2) {
        this.count = v1.getCount() + v2.getCount();
        this.value = v1.getValue() + v2.getValue();
    }

    public double getAvg() {
        return this.count / (double)this.value;
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
