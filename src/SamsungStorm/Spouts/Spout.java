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
  private DataInputStream din;
  private Socket socket;
  private int port;
  private String serverAddr;



  public Spout(String  addr , int port) {
    this.serverAddr = addr;
    this.port = port;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("xml"));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    System.out.println("Connection START");
    _collector = spoutOutputCollector;
    try {
      socket = new Socket(serverAddr , port);
      din = new DataInputStream(socket.getInputStream());
      System.out.println("Successfully conneted to " + socket.getInetAddress().getHostAddress());
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    try {
      short s = din.readShort();
      if(s == 0) {
        double x = din.readDouble();
        double y = din.readDouble();
        short end = din.readShort();

        if(end != -1) {
          System.out.println("TT");
        }
        _collector.emit(new Values(x , y));
      } else if(s == 1) {
        double minX = din.readDouble();
        double minY = din.readDouble();
        double maxX = din.readDouble();
        double maxY = din.readDouble();
        short end = din.readShort();

        if(end != -1) {
          System.out.println("TT");
        }
        _collector.emit(new Values(minX , minY , maxX , maxY));
      }
    }catch(Exception e){
      e.printStackTrace();
    }
  }



}
