package SamsungStorm.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 6
 * Time: 오후 2:47
 * To change this template use File | Settings | File Templates.
 */
public class RoundRobin  implements IRichBolt {

    OutputCollector _collector;

    private HashMap<String , String[]> inputs;
    private String[] routings;
    private String[] publish;
    public RoundRobin(String[] publish , HashMap<String , String[]> inputs,
                                 String[] routings){
        this.publish = publish;
        this.inputs = inputs;
        this.routings = routings;
    }

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }


    public void execute(Tuple tuple) {



    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> list = new ArrayList<String>();
        for(String s : routings){
            list.add(s);
        }
        list.add("query");
        list.add("isPub");
        list.add("xml");
        declarer.declare(new Fields(list));
    }

    public Map getComponentConfiguration(){
        return null;
    }
}
