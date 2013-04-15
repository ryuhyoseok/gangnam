package SamsungStorm.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 20
 * Time: 오후 7:40
 * To change this template use File | Settings | File Templates.
 */
public class RoundRobinQueryBolt implements IRichBolt {

    OutputCollector collector;
    List<GridCellElement>[][] grid;
    int gridSize = 512;

    public RoundRobinQueryBolt(int gridSize) {
      this.gridSize = gridSize;
      }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
      grid = new ArrayList[gridSize][gridSize];
      this.collector = outputCollector;
      //To change body of implemented methods use File | Settings | File Templates.
    }
//    public void execute(Tuple input) {
//
//    }
//
  @Override
  public void execute(Tuple tuple) {
    System.out.println("tupleInformations : " + tuple );
    boolean isPub = tuple.getBooleanByField("isPub");
    int i,j;
    int cnt = 0;

        /*
        int subId = input.getIntegerByField("query");
        double x = input.getDoubleByField("x");
        double y = input.getDoubleByField("y");

        int gridX;
        int gridY;

        GridCellElement subscription = new GridCellElement(subId, x, y);

        gridX = (int)x / gridSize;
        gridY = (int)y / gridSize;
        */

    if(isPub == false){
      long subId = tuple.getLongByField("query");
      double min_x = tuple.getDoubleByField("minx");
      double min_y = tuple.getDoubleByField("miny");
      double max_x = tuple.getDoubleByField("maxx");
      double max_y = tuple.getDoubleByField("maxy");
      String str  = tuple.getStringByField("xml");


//      System.out.println(subId + "==" + min_x + "==" + min_y + "==" + max_x + "==" + max_y);

      int gridMinX;
      int gridMinY;
      int gridMaxX;
      int gridMaxY;

      GridCellElement subscription = new GridCellElement(subId, min_x, min_y, max_x, max_y, str);

      gridMinX = (int)min_x / gridSize;
      gridMinY = (int)min_y / gridSize;
      gridMaxX = (int)max_x / gridSize;
      gridMaxY = (int)max_y / gridSize;

//      System.out.println(gridMinX + "----" + gridMinY + "----" + gridMaxX + "----" + gridMaxY);


      for(i = gridMinX; i < gridMaxX+1; i++){
        for(j = gridMinY; j < gridMaxY+1; j++){
          if(grid[i][j] == null)  {
            grid[i][j] = new ArrayList<GridCellElement>();
          }
          grid[i][j].add(subscription);
          //System.out.println(i + ", " + j + ", " + grid[i][j].size());
        }
      }

//      System.out.println();
//      System.out.println("=========New Query is registered============");
      int totalSize = 8 + 8 + 8 + 8 + 8 + str.getBytes().length;
//      System.out.println("Query ID : " + subId + "  minx : " + min_x + "  miny : " + min_y + "  maxx : " + max_x + "  maxy : " + max_y + " size " + totalSize);
    }

    else{

      long pubId = tuple.getLongByField("query");
      double x = tuple.getDoubleByField("x");
      double y = tuple.getDoubleByField("y");
      String str = tuple.getStringByField("xml");


      int gridX = (int)x / gridSize;
      int gridY = (int)y / gridSize;



//      System.out.println(x + "==" + y+ "==" + pubId + " , gridx : " +  gridX + " , " + gridY);


      if(grid[gridX][gridY] == null){
//        System.out.println("===============Not Matched==================");
      }

      else{
        Iterator<GridCellElement> iterator = grid[gridX][gridY].iterator();

        while(iterator.hasNext()){
          GridCellElement obj = iterator.next();

          if((obj.min_x <= x  && obj.max_x >= x) && (obj.min_y <= y && obj.max_y >= y)){
            cnt++;
//            System.out.println("=================Matched=================");
//            System.out.println("Query ID : " + obj.sub_id + "  minx : " + obj.min_x + "  miny : " + obj.min_y + "  maxx : " + obj.max_x + "  maxy : " + obj.max_y);
//            System.out.println("Publish ID : " + pubId + "  x : " + x + "  y : " + y);
//            System.out.println("=========================================");
          }
        }
        if(cnt == 0){
//          System.out.println("===============Not Matched cnt = 0==================");
        }

      }
    }

    //
  }



  public void cleanup() {

    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("exit"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
