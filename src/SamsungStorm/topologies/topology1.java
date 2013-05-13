package SamsungStorm.topologies;

import java.net.InetSocketAddress;
import java.util.HashMap;

import SamsungStorm.Bolts.*;
import SamsungStorm.Spouts.RRSubscriptionSpout;
import SamsungStorm.Spouts.RoundRobinSpout;
import SamsungStorm.Spouts.Spout;
import SamsungStorm.Spouts.SubscriptionSpout;
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
    if(args.length < 12) {
      System.out.println("TOPOLOGY_NAME  SERVERADDR   PUBPORT  SUBPORT  SPOUTNUM  RRNUM  GRIDSIZE  WORKERNUN LOCAL MAX_PENDING(0 IS NULL) SUBNUM  ROUTINGNUM");
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

    long subNum = Long.parseLong(args[10]);
    int routingNum = Integer.parseInt(args[11]);
    /**EXPERIMENT 1*/
//    builder.setSpout( "rrpub", new Spout(serverAddr , pubPort ), spoutNum );
//    builder.setSpout( "rrsub", new SubscriptionSpout(serverAddr , subPort , subNum ), spoutNum );
//    builder.setBolt("rrq" , new RoundRobinQueryBolt(gridSize) , rrNum).fieldsGrouping("rrpub" , new Fields("query")).allGrouping("rrsub");

    /*EXPERIMENT2*/
//    builder.setSpout( "rrpub", new Spout(serverAddr , pubPort ), spoutNum );
//    builder.setSpout( "rrsub", new SubscriptionSpout(serverAddr , subPort , subNum ), spoutNum );
//    builder.setBolt("rrq" , new RoundRobinQueryBolt(gridSize) , rrNum).fieldsGrouping("rrsub" , new Fields("query")).allGrouping("rrpub");

    /*EXPERIMENT3*/
    builder.setSpout( "rrpub", new Spout(serverAddr , pubPort ), spoutNum );
    builder.setSpout( "rrsub", new SubscriptionSpout(serverAddr , subPort , subNum ), spoutNum );
    builder.setBolt("spr" , new SpacePartitioningRoutingBolt3(gridSize , rrNum) , routingNum).shuffleGrouping("rrpub").shuffleGrouping("rrsub");
    builder.setBolt("spq" , new RoundRobinQueryBolt(gridSize) , rrNum).fieldsGrouping("spr", new Fields("node"));
//


//    builder.setBolt("spr" , new SpacePartitioningRoutingBolt2(gridSize , rrNum),routingNum).shuffleGrouping("rrpub").shuffleGrouping("rrsub");

//    for(int i = 0 ; i < rrs.length ; i ++) {
//      builder.setBolt("spq_" + i, new RoundRobinQueryBolt(gridSize) , 1).allGrouping("spr", rrs[i]);
//    }
//    for(int i = 0 ; i < rrs.length ; i ++) {
//      builder.setBolt("spq_"+i,new SpacePartitioningQueryBolt(gridSize) ,1 ).allGrouping("spr", rrs[i]);
//    }
//    for(int i = 0 ; i < rrs.length ; i ++) {
//      builder.setBolt("rrq_" + i, new RoundRobinQueryBolt(gridSize) ,1).allGrouping("rrpub", rrs[i]).allGrouping("rrsub");
//    }

    Config conf = new Config();
//    conf.setMaxSpoutPending(10);
    conf.setDebug(false);

    int maxPending = Integer.parseInt(args[9]);
    if(maxPending < 1) {
    } else {
      conf.setMaxSpoutPending(maxPending);
    }

    if (!local) {
      conf.setNumWorkers(workerNum);
      conf.setNumAckers(rrNum);
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
