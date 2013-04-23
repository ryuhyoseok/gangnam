package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 21
 * Time: 오후 3:30
 * To change this template use File | Settings | File Templates.
 */
public class FourthSubscriptionSpout extends BaseRichSpout {

    private SpoutOutputCollector _collector;
//    private BufferedReader reader;
    private DataInputStream din;
    private Socket socket;
    private int port;
    int nodeNum;
    private String serverAddr;
    private static final String str;
    static {
      StringBuilder builder = new StringBuilder();
      for(int i = 0 ; i < 1024 ; i ++ ) {
        builder.append('a');
      }
      str = builder.toString();
    }

    public FourthSubscriptionSpout(String addr, int port, int nodeNum) {
        this.serverAddr = addr;
        this.port = port;
        this.nodeNum = nodeNum;
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
//        reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        din = new DataInputStream(socket.getInputStream());
        System.out.println("Successfully conneted to " + socket.getInetAddress().getHostAddress());
      } catch(IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void nextTuple() {
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

            if(pubFlag == -2){
                int sum = din.readInt();
                int perNode = sum / nodeNum;
                _collector.emit(new Values(pubFlag, sum, perNode, 0, 0, 0, false));
            }

            if(pubFlag == -3){
                int gridCellNum = din.readInt();
                int count = din.readInt();
                _collector.emit(new Values(pubFlag, gridCellNum, count, 0, 0, 0, false));

            }
              else{
                    long id = din.readLong();
                    double minX = din.readDouble();
                    double minY = din.readDouble();
                    double maxX = din.readDouble();
                    double maxY = din.readDouble();
                    short endFlag = din.readShort();
                    if(pubFlag != 1) {
                        throw new Exception("wrong data input!  pubflag = " + pubFlag);
                    }
                    if(endFlag != -1) {
                        throw new Exception("wrong data end! end(pub)Flag = " + endFlag);
                    }
                    _collector.emit(new Values(id , minX , minY, maxX , maxY, str , false));
            }


        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
