package SamsungStorm.topologies;

import java.net.InetSocketAddress;
import java.util.HashMap;

import SamsungStorm.Bolts.*;
import SamsungStorm.Spouts.RRSubscriptionSpout;
import SamsungStorm.Spouts.RoundRobinSpout;
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
    if(args.length < 8) {
      System.out.println("TOPOLOGY_NAME  SERVERADDR   PUBPORT  SUBPORT  SPOUTNUM   RRNUM  GRIDSIZE  WORKERNUN LOCAL");
      System.exit(0);
    }

    Format format = new Format();
    HashMap<String , String[]> routingMap = format.getRoutingSchema() ;
    String[] routing = routingMap.keySet().toArray(new String[0]);

    String topol = args[0];
    String serverAddr = args[1];
    int pubPort = Integer.parseInt(args[2]);
    int subPort = Integer.parseInt(args[3]);
    int spoutNum = Integer.parseInt(args[4]);
    int rrNum = Integer.parseInt(args[5]);
    int gridSize = Integer.parseInt(args[6]);
    int workerNum = Integer.parseInt(args[7]);
    boolean local = Boolean.parseBoolean(args[8]);
    String[] rrs = new String[rrNum];
    for(int i = 0 ; i < rrNum ; i ++) {
      rrs[i] = String.valueOf(i);
    }
    System.out.println(topol);
    System.out.println(serverAddr);
    System.out.println(pubPort);
    System.out.println(subPort);

    TopologyBuilder builder = new TopologyBuilder();

//    builder.setSpout( "rrpub", new RoundRobinSpout(serverAddr , pubPort, rrs), spoutNum );
    builder.setSpout( "rrsub", new RRSubscriptionSpout(serverAddr , subPort, rrs), spoutNum );
    for(int i = 0 ; i < rrs.length ; i ++) {
      builder.setBolt("rrq_" + i, new RoundRobinQueryBolt(gridSize)).allGrouping("rrsub");
    }

    Config conf = new Config();
    conf.setDebug(false);

    if (!local) {
      conf.setNumWorkers(workerNum);
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    } else {
      conf.setMaxTaskParallelism(workerNum);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(100000);

      cluster.shutdown();
    }
  }
}
