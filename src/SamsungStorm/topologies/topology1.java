package SamsungStorm.topologies;

import java.net.InetSocketAddress;
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
    if(args.length < 7) {
      System.out.println(" TOPOLOGY_NAME   SERVERADDR   PORT    SPOUTNUM    FEATURENUM    QUERYNUM  LOCAL");
//      System.exit(0);
    }

    Format format = new Format();
    HashMap<String , String[]> routingMap = format.getRoutingSchema() ;
    String[] routing = routingMap.keySet().toArray(new String[0]);

    String topol = args[0];
    String serverAddr = args[1];
    int port = Integer.parseInt(args[2]);
    int spoutNum = Integer.parseInt(args[3]);
    int featureNum = Integer.parseInt(args[4]);
    int queryNum = Integer.parseInt(args[5]);
    boolean local = Boolean.parseBoolean(args[6]);
    System.out.println(topol);
    System.out.println(serverAddr);
    System.out.println(port);


    TopologyBuilder builder = new TopologyBuilder();


    builder.setSpout( "Spout", new Spout(serverAddr , port), spoutNum );

//    builder.setBolt("parseBolt", new FeatureExtractionBolt( new String[]{} ,format.getInputSchema() , routing), featureNum ).shuffleGrouping("Spout");
//
//    builder.setBolt("routingboltA", new RoutingBolt(routing, routing[0], routingMap.get(routing[0])), 1).shuffleGrouping("parseBolt");
//
//    builder.setBolt("routingboltB", new RoutingBolt(routing, routing[1], routingMap.get(routing[1])), 1).fieldsGrouping("routingboltA", new Fields(routing[0]));
//
//    builder.setBolt("routingboltE", new RoutingBolt(routing, routing[2], routingMap.get(routing[2])), 1).fieldsGrouping("routingboltB", new Fields(routing[1]));
//
//    builder.setBolt("QueryBolt", new QueryBolt(),1).fieldsGrouping("routingboltB", new Fields(routing[2]));

    Config conf = new Config();
    conf.setDebug(false);


    if (!local) {
      conf.setNumWorkers(1);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(100000);

      cluster.shutdown();
    }
  }
}
