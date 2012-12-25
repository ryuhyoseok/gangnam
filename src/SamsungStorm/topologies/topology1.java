package SamsungStorm.topologies;

import java.util.HashMap;

import SamsungStorm.Bolts.*;
import SamsungStorm.Spouts.Spout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 19
 * Time: 오후 4:02
 * To change this template use File | Settings | File Templates.
 */
public class topology1 {

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("Spout", new Spout(), 1);

    
    HashMap<String , String[]> input = new HashMap<String , String[]> ();
    
    input.put("A", new String[]{"1", "2", "3", "4"});
    input.put("B", new String[]{"1", "2", "3", "4"});
    input.put("E", new String[]{"1", "2", "3", "4"});
    
    builder.setBolt("parseBolt", new FeatureExtractionBolt(new String[]{} ,input , new String[]{"A", "B", "E"}), 1).globalGrouping("Spout");

    builder.setBolt("routingboltA", new RoutingBolt(new String[]{"A", "B", "E"}, "A", new String[]{"1", "2"}), 1).shuffleGrouping("parseBolt");
    
    builder.setBolt("routingboltB", new RoutingBolt(new String[]{"A", "B", "E"}, "B", new String[]{"1", "2", "3"}), 1).fieldsGrouping("routingboltA", new Fields("A"));

    builder.setBolt("routingboltE", new RoutingBolt(new String[]{"A", "B", "E"}, "E", new String[]{"1", "2", "3", "4"}), 1).fieldsGrouping("routingboltB", new Fields("B"));

    builder.setBolt("QueryBolt", new QueryBolt(),1).fieldsGrouping("routingboltB", new Fields("E"));
/*
builder.setBolt("AggregationBolt", new AggregationBolt(), 10)
.shuffleGrouping("Middle","area,x,y");

builder.setBolt("QuantityBolt", new QuantityBolt(), 10)
.shuffleGrouping("Middle","area,val");*/
    Config conf = new Config();
    conf.setDebug(true);


    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(100000);

      cluster.shutdown();
    }
  }
}
