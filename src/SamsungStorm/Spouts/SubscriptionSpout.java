package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

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
    private ObjectInputStream oin;
    private Socket socket;
    private int port;
    private String serverAddr;



    public SubscriptionSpout(String  addr , int port) {
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
            _collector.emit(new Values(s));

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}