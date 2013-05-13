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

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 20
 * Time: 오후 7:40
 * To change this template use File | Settings | File Templates.
 */
public class SpacePartitioningQueryBolt extends BaseBasicBolt {

    HashMap<Integer, List<GridCellElement>> gridIndex;
    List<GridCellElement> hilbert;
    long totalCounter = 0;
    long matchedCounter = 0;
    int gridSize;



    public SpacePartitioningQueryBolt (int gridSize) {
        this.gridSize = gridSize;

    }

    public void prepare(java.util.Map stormConf, backtype.storm.task.TopologyContext context) {
      this.gridIndex = new HashMap<Integer, List<GridCellElement>>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
//        System.out.println(tuple);
//        System.out.println(tuple);
        totalCounter ++;
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
            int nodeNum = tuple.getIntegerByField("node");
            int gridCellNum = tuple.getIntegerByField("gridcell");


            GridCellElement subscription = new GridCellElement(subId, min_x, min_y, max_x, max_y, str);

            if(!gridIndex.containsKey(gridCellNum)){
                List<GridCellElement> query = new ArrayList<GridCellElement>();
                query.add(subscription);
                gridIndex.put(gridCellNum, query);
//                System.out.println("New Query is coming!! "+ gridCellNum + " ID : " + gridIndex.get(gridCellNum).get(0).sub_id);

                System.out.println("Subscription " +
                  "TUPLE:["+tuple.getSourceStreamId() +","+ tuple.getSourceComponent() +
                  "][" + subId + "," + min_x + "," + min_y + "," + max_x + "," + max_y + "," +  isPub + "]");

            }

            else{
                gridIndex.get(gridCellNum).add(subscription);
//                System.out.println("QueryQueryQuery " + gridCellNum + "ID : " + gridIndex.get(gridCellNum).get(1).sub_id);
                System.out.println("Subscription " +
                    "TUPLE:["+tuple.getSourceStreamId() +","+ tuple.getSourceComponent() +
                    "][" + subId + "," + min_x + "," + min_y + "," + max_x + "," + max_y + "," +  isPub + "]");
            }



            /*
            System.out.println();
            System.out.println("=========New Query is registered============");
            System.out.println("Query ID : " + subId + "  minx : " + min_x + "  miny : " + min_y + "  maxx : " + max_x + "  maxy : " + max_y);
            */

        }

        else{

            long pubId = tuple.getLongByField("query");
            double x = tuple.getDoubleByField("x");
            double y = tuple.getDoubleByField("y");
            String str = tuple.getStringByField("xml");
            int nodeNum = tuple.getIntegerByField("node");
            int gridCellNum = tuple.getIntegerByField("gridcell");


            if(!gridIndex.containsKey(gridCellNum)){
//                System.out.println("===============Not Matched==================");
            }

            else{
                List<GridCellElement> queryList = gridIndex.get(gridCellNum);

                Iterator<GridCellElement> iterator = queryList.iterator();

                while(iterator.hasNext()){
                    GridCellElement obj = iterator.next();

                    if((obj.min_x <= x  && obj.max_x >= x) && (obj.min_y <= y && obj.max_y >= y)){
                        cnt++;
//                        System.out.println("=================Matched=================");
//                        System.out.println("Query ID : " + obj.sub_id + "  minx : " + obj.min_x + "  miny : " + obj.min_y + "  maxx : " + obj.max_x + "  maxy : " + obj.max_y);
//                        System.out.println("Publish ID : " + pubId + "  x : " + x + "  y : " + y);
//                        System.out.println("=========================================");
                    }
                }
            }

          if(cnt == 0){
//          System.out.println("===============Not Matched cnt = 0==================");
            System.out.println("publish " + "TUPLE:["+tuple.getSourceStreamId() +","+ tuple.getSourceComponent()
                +"] ["+ pubId + "," + x + "," + y + ","  +  isPub + "]"  +
                " NOT TOTAL:" + totalCounter);
          }else {
            matchedCounter ++;
            System.out.println("Publish " + "TUPLE:["+tuple.getSourceStreamId() +","+ tuple.getSourceComponent() +
                "][" + pubId + "," + x + "," + y + ","  +  isPub + "]"  +
                "MATCHED  MATCHCOUNTER:" + matchedCounter + " TOTAL:" + totalCounter);
          }

            /*
            if(grid[gridX][gridY] == null){
                System.out.println("===============Not Matched==================");
            }

            else{
                Iterator<GridCellElement> iterator = grid[gridX][gridY].iterator();

                while(iterator.hasNext()){
                    GridCellElement obj = iterator.next();

                    if((obj.min_x <= x  && obj.max_x >= x) && (obj.min_y <= y && obj.max_y >= y)){
                        cnt++;
                        System.out.println("=================Matched=================");
                        System.out.println("Query ID : " + obj.sub_id + "  minx : " + obj.min_x + "  miny : " + obj.min_y + "  maxx : " + obj.max_x + "  maxy : " + obj.max_y);
                        System.out.println("Publish ID : " + pubId + "  x : " + x + "  y : " + y);
                        System.out.println("=========================================");
                    }
                }
                if(cnt == 0){
                    System.out.println("===============Not Matched cnt = 0==================");
                }

            }
            */
        }
//      collector.emit(tuple , new Values(""));
      basicOutputCollector.emit(new Values(""));

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
