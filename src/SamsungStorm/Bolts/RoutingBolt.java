package SamsungStorm.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RoutingBolt implements IRichBolt {

  private OutputCollector collector;
  private String targetAttr;
  private String[] targetRange;
  private String[] routingAttrs;
  
  public RoutingBolt(String[] routingAttrs , 
      String targetAttr , String[] targetRange) throws Exception{
    this.routingAttrs = routingAttrs;
    this.targetAttr = targetAttr;
    this.targetRange = targetRange;
    if(routingAttrs == null || targetAttr == null || targetRange == null 
        || targetRange.length < 1) {
      throw new Exception("Invalid Routing schema ");
    }
  }
  
  public void prepare(Map stormConf, TopologyContext context,
      OutputCollector collector) {
    this.collector = collector;
  }
  
  public void execute(Tuple input) {
    String value = input.getStringByField(targetAttr);
    if(value.equals(Constants.DONCARE)) {
      try {
      //copy to all next bolt
        ArrayList<Object> arrayList = new ArrayList<Object>(input.size());
        if(this.routingAttrs.length != input.size()) {
            throw new Exception("The size of routing schema does not match to the size of tuple , "
                  + "the size of routing schema is " + this.routingAttrs.length + 
                  " ,  the size of tuple is " + input.size());
          }
        /*for(String attr : this.routingAttrs) {
          arrayList.add(input.getValueByField(attr));
        }*/

        Values newval = new Values(input);
        for(String target : targetRange){
          newval.set(input.fieldIndex("targetAttr"), target);
          collector.emit(newval);
        }
      } catch (Exception e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
       }
      
    } else {

      Values val = new Values();
      for(Object o : input.getValues()){
        val.add(o);
      }
      collector.emit(val);
    }
  }
  
  
  public void cleanup() {
  }
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    List<String> list = new ArrayList<String>();
    for(String s : this.routingAttrs){
      list.add(s);
    }
    list.add("query");
    list.add("isPub");
    list.add("xml");
    declarer.declare(new Fields(list));
  }
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }

}
