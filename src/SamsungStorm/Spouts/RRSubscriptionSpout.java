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
 * User: sungmin
 * Date: 13. 3. 21
 * Time: 오후 3:30
 * To change this template use File | Settings | File Templates.
 */
public class RRSubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private DataInputStream din;
//    private BufferedReader reader;
    private Socket socket;
    private int port;
    private String serverAddr;
    private static final String str;
    private String[] rrs;
    private int counter;
    private long totalCounter = 0;
    private long maximum = 0;
    static {
      StringBuilder builder = new StringBuilder();
      for(int i = 0 ; i < 1024 ; i ++ ) {
        builder.append('a');
      }
      str = builder.toString();
    }

    public RRSubscriptionSpout(String addr, int port , String[] rrs , long maximum) {
        this.serverAddr = addr;
        this.port = port;
        this.rrs = rrs;
        counter = 0;
        this.maximum = maximum;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
      for(int i = 0 ; i < rrs.length ; i ++) {
        outputFieldsDeclarer.declareStream(rrs[i],new Fields("query" , "minx" , "miny" , "maxx" , "maxy" , "xml" ,"isPub"));
      }
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
      System.out.println("Connection START :" +  getClass().getName());
      _collector = spoutOutputCollector;
      try {
        socket = new Socket(serverAddr , port);
        din = new DataInputStream(socket.getInputStream());
//        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        System.out.println("Successfully conneted to " + socket.getInetAddress().getHostAddress() + " , time : " + System.currentTimeMillis());
      } catch(IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void nextTuple() {
      if(totalCounter >= maximum) {
        try{
          Thread.sleep(1000);
        }  catch(Exception e) {
          e.printStackTrace();
        }
      } else {
        try {
//          String[] strs = reader.readLine().split(",");
//          short pubFlag = Short.parseShort(strs[0]);
//          long id = Long.parseLong(strs[1]);
//          double minX = Double.parseDouble(strs[2]);
//          double minY = Double.parseDouble(strs[3]);
//          double maxX = Double.parseDouble(strs[4]);
//          double maxY = Double.parseDouble(strs[5]);
//          short endFlag = Short.parseShort(strs[6]);

          short pubFlag = din.readShort();
          long id = din.readLong();
          double minX = din.readDouble();
          double minY = din.readDouble();
          double maxX = din.readDouble();
          double maxY = din.readDouble();
          short endFlag = din.readShort();

          if(pubFlag != 1) {
            throw new Exception("wrong data input!  subflag = " + pubFlag);
          }
          if(endFlag != -1) {
            throw new Exception("wrong data end! end(sub)Flag = " + endFlag);
          }

//          _collector.emit(new Values(id ,minX , minY, maxX , maxY, str,false ));
          Values value = new Values(id , minX , minY, maxX, maxY, str,false );
          _collector.emit(rrs[counter%rrs.length] , value );
//          System.out.println("STEAMID:" + rrs[counter%rrs.length] + " TUPLE:[" + id + "," + minX + "," + minY + "," + maxX + "," + maxY + "," +  false + "]");
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
}
