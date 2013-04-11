package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 23
 * Time: 오후 2:53
 * To change this template use File | Settings | File Templates.
 */
public class RoundRobinSpout extends BaseRichSpout {

  public static final String RR = "rr";
  private SpoutOutputCollector _collector;
  private DataInputStream din;
  private BufferedReader reader;
  private Socket socket;
  private int port;
  private String serverAddr;
  private static final String str;
  private String[] rrs;
  private int counter;

  static {
    StringBuilder builder = new StringBuilder();
    for(int i = 0 ; i < 1024 ; i ++ ) {
      builder.append('a');
    }
    str = builder.toString();
  }

  public RoundRobinSpout(String addr, int port , String[] rrs) {
    this.serverAddr = addr;
    this.port = port;
    this.rrs = rrs;
    counter = 0;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    for(int i = 0 ; i < rrs.length ; i ++) {
      outputFieldsDeclarer.declareStream(rrs[i] , new Fields("query","x","y","xml","isPub"));
    }
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    System.out.println("Connection START : " + getClass().getName());
    _collector = spoutOutputCollector;
    try {
      socket = new Socket(serverAddr , port);
      din = new DataInputStream(socket.getInputStream());
//      reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
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
      _collector.emit(rrs[counter%rrs.length] ,new Values(id , x , y, str,true ));
      if(counter == rrs.length -1) {
        counter = 0;
      }else {
        counter ++;
      }

    }catch(Exception e){
      e.printStackTrace();
    }
  }



}
