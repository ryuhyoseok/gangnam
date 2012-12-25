package SamsungStorm.Bolts;

import java.util.Hashtable;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class QueryBolt implements IRichBolt{

	OutputCollector collector;
	Hashtable<Integer, String> QueryTable;
	//public QueryBolt(){
		
	//}
	
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		QueryTable = new Hashtable<Integer, String> ();
		this.collector = collector; 		
	}

	
	public void execute(Tuple input) {
		
				
		boolean isPub = input.getBooleanByField("isPub");
		String value = input.getStringByField("xml");
		int query = input.getIntegerByField("query");
		
		if(isPub == false){
			if(QueryTable.containsKey(query)){
				System.out.println("**********New Subscriber is registered*****************");
			}
			else{
				QueryTable.put(query, value);
				System.out.println("------------New Query is registered---------------");
			}
		}
		
		else{
			if(QueryTable.containsKey(query)){
				System.out.println(query+"===============Publish==================");
				System.out.println(value);
				System.out.println("============================================");
			}
			else{
				System.out.println(query+"^^^^^^^^^^^^^^^Publish^^^^^^^^^^^^^^^^^^");
			}
		}
		
		//
	}

	
	public void cleanup() {
		
	}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
	

}
