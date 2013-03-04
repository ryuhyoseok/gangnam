package SamsungStorm.Bolts;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 19
 * Time: 오후 7:10
 * To change this template use File | Settings | File Templates.
 */
public class FeatureExtractionBolt extends BaseBasicBolt {
  private HashMap<String , String[]> inputs;
  private String[] routings;
  private String[] publish;
  public FeatureExtractionBolt(String[] publish , HashMap<String , String[]> inputs,
      String[] routings){
    this.publish = publish;
    this.inputs = inputs;
    this.routings = routings;
  }

  @Override
  public void cleanup() {
    super.cleanup();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Values values = new Values();
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder parser;

    int query = 0;
    boolean isPub = true;
    ByteArrayInputStream is = new ByteArrayInputStream(tuple.getString(0).getBytes());
    try {
      parser = factory.newDocumentBuilder();
      Document document = parser.parse(is);
      Set<String> inputSet = inputs.keySet();
      
      for(String inputSchema : inputSet) {
        NodeList nodes = document.getElementsByTagName(inputSchema);
        if(nodes.getLength() > 0){
          String content = nodes.item(0).getTextContent();
          String[] ranges = inputs.get(inputSchema);
          int counter = 0;
          for(;counter < ranges.length ; counter ++) {
            if(ranges[counter].equals(content)){
              break;
            }
          }
          query += counter;
          query = query * 10;
        }
        
      }
      
      for(String tag : routings){
        NodeList nodes = document.getElementsByTagName(tag);
        if(nodes.getLength() > 0){
          String content = nodes.item(0).getTextContent();
          if(content.equals("")) {
            values.add(Constants.DONCARE);
          }else {
          values.add(content);
          }
        }
      }
      
      if(document.getDocumentElement().getTagName().equals("publish")) {
        isPub = true;
      }else if(document.getDocumentElement().getTagName().equals("subscribe")) {
        isPub = false;
      }else{
        throw new Exception("WRONG STATEMENT  " + tuple.getString(0));
      }
      
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    values.add(query);
    values.add(isPub);
    values.add(tuple.getString(0));
    collector.emit(values);
  }

  @Override
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
}
