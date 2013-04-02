package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 21
 * Time: 오후 3:30
 * To change this template use File | Settings | File Templates.
 */
public class SubscriptionSpout extends BaseRichSpout {

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

    public SubscriptionSpout(String  addr , int port) {
        this.serverAddr = addr;
        this.port = port;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("query" , "minx" , "miny" , "maxx" , "maxy" , "xml" , "isPub"));
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
          short pubFlag = din.readShort();
          if(pubFlag != 0) {
            throw new Exception("wrong data input!  pubflag = " + pubFlag);
          }
          long id = din.readShort();
          double minX = din.readShort();
          double minY = din.readShort();
          double maxX = din.readShort();
          double maxY = din.readShort();
          short endFlag = din.readShort();
          if(endFlag != -1) {
            throw new Exception("wrong data end! endFlag = " + endFlag);
          }
          _collector.emit(new Values(id , minX , minY, maxX , maxY, str , false));

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
