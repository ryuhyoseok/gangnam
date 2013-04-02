package SamsungStorm.Bolts;

/**
 * Created with IntelliJ IDEA.
 * User: sungmin
 * Date: 13. 3. 28
 * Time: 오후 8:24
 * To change this template use File | Settings | File Templates.
 */
public class GridCellElement {
    public long sub_id;
    public double min_x;
    public double min_y;
    public double max_x;
    public double max_y;
    public String str;


    public GridCellElement(long sub_id, double min_x, double min_y, double max_x, double max_y, String str){
        this.sub_id = sub_id;
        this.min_x = min_x;
        this.min_y = min_y;
        this.max_x = max_x;
        this.max_y = max_y;
        this.str = str;
    }
}
