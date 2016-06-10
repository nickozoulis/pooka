package speed.storm.topology;

import backtype.storm.topology.TopologyBuilder;

import java.util.Properties;

/**
 * Created by nickozoulis on 10/06/2016.
 */
abstract class TopologyFactory<T> {
    abstract T getTopology();
}
