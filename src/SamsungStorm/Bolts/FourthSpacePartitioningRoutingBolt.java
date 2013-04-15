package SamsungStorm.Bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 20
 * Time: 오후 7:40
 * To change this template use File | Settings | File Templates.
 */
public class FourthSpacePartitioningRoutingBolt implements IRichBolt {

    OutputCollector collector;
    List<GridCellElement>[][] grid;
    HashMap<Integer, Integer> hilbertHash;
    int gridSize = 512;
    int cluster;


    public FourthSpacePartitioningRoutingBolt(int gridSize, int cluster) {
        this.gridSize = gridSize;
        this.cluster = cluster;
    }

    public void prepare(Map stormConf, TopologyContext context,
                        OutputCollector collector) {
        System.out.println(getClass().getName() + " PREPARE START");
        int i;
        char flag = 'a';
        char curflag = 'a';
        int hilbertIndex = 0;

        int x = 0, y = 0;
        int cur_x = 0;
        int cur_y =0;
        int key;
        int nodeNum;
        int nodeSize;

        int level = 0;
        int tmp = 100;
        int gridSumSize = gridSize * gridSize;

        nodeSize = gridSumSize / cluster;

        grid = new ArrayList[gridSize][gridSize];

        hilbertHash = new HashMap<Integer, Integer>(gridSumSize);

        int tmpGrid = gridSize;
        while(tmp >= 1){
            tmp = tmpGrid / 2;
            tmpGrid = tmpGrid/2;
            level++;
        }

        for(x = 0; x<gridSize; x++){
            for(y=0; y<gridSize; y++){
                key = x + gridSize*y;
                hilbertIndex = 0;
                cur_x = 0;
                cur_y = 0;
                while(level != 0){
                    switch (curflag){
                        case 'a' :
                            if (x < cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'b';

                            }

                            else if (x < cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'a';
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (1*(int)Math.pow(4, level-1));
                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'c';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (2*(int)Math.pow(4, level-1));

                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'd';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (3*(int)Math.pow(4, level-1));

                            }
                            break;

                        case 'b' :
                            if (x < cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'a';

                            }

                            else if (x < cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'd';
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (1*(int)Math.pow(4, level-1));
                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'b';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (2*(int)Math.pow(4, level-1));

                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'b';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (3*(int)Math.pow(4, level-1));

                            }
                            break;

                        case 'c' :
                            if (x < cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'c';

                            }

                            else if (x < cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'c';
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (1*(int)Math.pow(4, level-1));
                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'd';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (2*(int)Math.pow(4, level-1));

                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'a';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (3*(int)Math.pow(4, level-1));

                            }
                            break;

                        case 'd' :
                            if (x < cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'd';

                            }

                            else if (x < cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'b';
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (1*(int)Math.pow(4, level-1));
                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y >= cur_y + Math.pow(2, level-1)){
                                curflag = 'c';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                cur_y = cur_y + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (2*(int)Math.pow(4, level-1));

                            }

                            else if (x >= cur_x + Math.pow(2, level-1) && y < cur_y + Math.pow(2, level-1)){
                                curflag = 'd';
                                cur_x = cur_x + (int)Math.pow(2, level-1);
                                hilbertIndex = hilbertIndex + (3*(int)Math.pow(4, level-1));

                            }
                            break;
                      }
                  level --;
                }
                nodeNum = (hilbertIndex+1) / nodeSize;
                if(nodeNum == cluster){
                    nodeNum = nodeNum-1;
                }
                hilbertHash.put(key, nodeNum);

            }
        }

        for(i=0;i<100;i++){
            System.out.println(i + "============" + i + "%%%%%%%%%%%%%%%%%%%%%%%%%%%" + hilbertHash.get(i) + "***************");
        }
        for(i=20000;i<20100;i++){
            System.out.println(i + "============" + i + "%%%%%%%%%%%%%%%%%%%%%%%%%%%" + hilbertHash.get(i) + "***************");
        }


        this.collector = collector;

    }


    public void execute(Tuple input) {


        boolean isPub = input.getBooleanByField("isPub");
        int i,j;
        int cnt = 0;
        int gridCellNum;
        int nodeNum=0;

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
            long subId = input.getLongByField("query");
            double min_x = input.getDoubleByField("minx");
            double min_y = input.getDoubleByField("miny");
            double max_x = input.getDoubleByField("maxx");
            double max_y = input.getDoubleByField("maxy");
            String str  = input.getStringByField("xml");


            System.out.println(subId + "==" + min_x + "==" + min_y + "==" + max_x + "==" + max_y);

            int gridMinX;
            int gridMinY;
            int gridMaxX;
            int gridMaxY;

            GridCellElement subscription = new GridCellElement(subId, min_x, min_y, max_x, max_y, str);

            gridMinX = (int)min_x / gridSize;
            gridMinY = (int)min_y / gridSize;
            gridMaxX = (int)max_x / gridSize;
            gridMaxY = (int)max_y / gridSize;

            System.out.println(gridMinX + "----" + gridMinY + "----" + gridMaxX + "----" + gridMaxY);


            for(i = gridMinX; i < gridMaxX+1; i++){
                for(j = gridMinY; j < gridMaxY+1; j++){
                    gridCellNum = i + gridSize*j;
                    nodeNum = hilbertHash.get(gridCellNum);
                    Values val = new Values();
                    val.add(subId);
                    val.add(min_x);
                    val.add(min_y);
                    val.add(max_x);
                    val.add(max_y);
                    val.add(str);
                    val.add(isPub);
                    val.add(nodeNum);
                    val.add(gridCellNum);
                    //System.out.println("gridcellnum : " + gridCellNum + ", " + "nodeNum : " + nodeNum);
                    collector.emit("sub", val);

                }
            }

//            System.out.println();
            //System.out.println("=========New Query is registered============");
            //System.out.println("Query ID : " + subId + "  minx : " + min_x + "  miny : " + min_y + " maxx : " + max_x + "  maxy : " + max_y);

        }

        else{

            long pubId = input.getLongByField("query");

            double x = input.getDoubleByField("x");
            double y = input.getDoubleByField("y");
            String str = input.getStringByField("xml");


            int gridX = (int)x / gridSize;
            int gridY = (int)y / gridSize;

            gridCellNum = gridX + gridSize*gridY;



            System.out.println(x + "==" + y+ "==" + pubId + " , grid : " +  gridX + " , " + gridY + "gridcellNum : " + gridCellNum);


            nodeNum = hilbertHash.get(gridCellNum);

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

            Values val = new Values();
            val.add(pubId);
            val.add(x);
            val.add(y);
            val.add(str);
            val.add(isPub);
            val.add(nodeNum);
            val.add(gridCellNum);
            collector.emit("pub", val);

        }

        //
    }


    public void cleanup() {

    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("pub" , new Fields("query" , "x" , "y" ,  "xml" , "isPub" , "node" , "gridcell"));
        declarer.declareStream("sub" , new Fields("query" , "minx" , "miny" , "maxx" , "maxy" , "xml" , "isPub" , "node" , "gridcell"));
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


}
