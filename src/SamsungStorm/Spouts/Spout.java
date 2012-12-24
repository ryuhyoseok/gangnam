package SamsungStorm.Spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.*;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: byungnam
 * Date: 12. 11. 23
 * Time: 오후 2:53
 * To change this template use File | Settings | File Templates.
 */
public class Spout extends BaseRichSpout {

  SpoutOutputCollector _collector;


  @Override
  public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields("xml"));
  }

  @Override
  public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
    _collector = spoutOutputCollector;

  }

  @Override
  public void nextTuple() {
    Utils.sleep(1000);
    String xml = new String("<Doc><A>1</A><B>2</B><C><D>3</D><E>4</E></C></Doc>");

    _collector.emit(new Values(xml));
  }
}
