package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.*;
import java.net.*;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 23
 * Time: 오후 2:53
 * To change this template use File | Settings | File Templates.
 */
public class Spout extends BaseRichSpout {

  private SpoutOutputCollector _collector;
  private ObjectInputStream oin;
  private Socket socket;
  private int port;
  private String serverAddr;
    private DataInputStream din;



  public Spout(String  addr , int port) {
    this.serverAddr = addr;
    this.port = port;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("id","x","y","xml"));

  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    System.out.println("Connection START");
    _collector = spoutOutputCollector;
    try {
      socket = new Socket(serverAddr , port);
      oin = new ObjectInputStream(socket.getInputStream());
      System.out.println("Successfully conneted to " + socket.getInetAddress().getHostAddress());
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    try {
        String s = (String)(oin.readObject());
        long id = 0;
        double x = 0;
        double y = 0;
        _collector.emit(new Values(id , x , y, s));


    }catch(Exception e){
      e.printStackTrace();
    }
  }
}
