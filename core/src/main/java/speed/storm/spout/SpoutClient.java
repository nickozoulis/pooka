package speed.storm.spout;

/**
 * Created by nickozoulis on 10/06/2016.
 */
interface SpoutClient<T> {
    T getSpout();
}
