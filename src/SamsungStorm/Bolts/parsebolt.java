package SamsungStorm.Bolts;

import backtype.storm.topology.BasicOutputCollector;
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
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 19
 * Time: 오후 7:10
 * To change this template use File | Settings | File Templates.
 */
public class parsebolt extends BaseBasicBolt {
  String[] tags;
  public parsebolt(String[] tags){
    this.tags = tags;
  }
//  public Fields getTags(String[] tags){
//    return new Fields(tags);
//  }

  @Override
  public void cleanup() {
    super.cleanup();    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Values values = new Values();
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder parser;

    ByteArrayInputStream is = new ByteArrayInputStream(tuple.getString(0).getBytes());
    try {
      parser = factory.newDocumentBuilder();
      Document document = parser.parse(is);
      for(String tag : tags){
        NodeList nodes = document.getElementsByTagName(tag);
        if(nodes.getLength() > 0){
          String content = nodes.item(0).getTextContent();
          values.add(content);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    values.add(tuple.getString(0));
    collector.emit(values);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    List<String> list = new ArrayList<String>();
    for(String s : tags){
      list.add(s);
    }
    list.add("xml");
    declarer.declare(new Fields(list));
  }

}
