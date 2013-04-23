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

//  private BufferedReader reader;
  private SpoutOutputCollector _collector;
  private DataInputStream din;
  private Socket socket;
  private int port;
  private String serverAddr;
  private static final String str;

  static {
    StringBuilder builder = new StringBuilder();
    for(int i = 0 ; i < 1024 ; i ++ ) {
      builder.append('a');
    }
    str = builder.toString();
  }

  public Spout(String  addr , int port) {
    this.serverAddr = addr;
    this.port = port;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("query","x","y","xml","isPub"));

  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    System.out.println("Connection START");
    _collector = spoutOutputCollector;
    try {
      socket = new Socket(serverAddr , port);
//      reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      din = new DataInputStream(socket.getInputStream());
      System.out.println("Successfully conneted to " + socket.getInetAddress().getHostAddress());
    } catch(IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void nextTuple() {
    try {
//      String[] strs = reader.readLine().split(",");
//      short pubFlag = Short.parseShort(strs[0]);
//      long id = Long.parseLong(strs[1]);
//      double x = Double.parseDouble(strs[2]);
//      double y = Double.parseDouble(strs[3]);
//      short endFlag = Short.parseShort(strs[4]);
      short pubFlag = din.readShort();
      long id = din.readLong();
      double x = din.readDouble();
      double y = din.readDouble();
      short endFlag = din.readShort();

      if(pubFlag != 0) {
        throw new Exception("wrong data input!  pubflag = " + pubFlag);
      }
      if(endFlag != -1) {
        throw new Exception("wrong data end! end(pub)Flag = " + endFlag);
      }
//      System.out.println("PUBBBBB : " + id + " , " + x + " , " + y );
      _collector.emit(new Values(id , x , y, str,true) , id);

    }catch(Exception e){
      e.printStackTrace();
    }
  }



}
