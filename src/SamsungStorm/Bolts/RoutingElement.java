package SamsungStorm.Bolts;

import java.util.ArrayList;

public class RoutingElement {
  
  private String dataType;
  private String fieldName;
  private ArrayList<String> routingRange;
  
  public RoutingElement(String fieldName , 
      String dataType , ArrayList<String> routingRange) {
    this.dataType = dataType;
    this.fieldName = fieldName;
    this.routingRange = routingRange;
  }
  
  public String getDataType() {
    return dataType;
  }
  
  public String getFieldName() {
    return fieldName;
  }
  
  public ArrayList<String> getRoutingRange() {
    return routingRange;
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("[ RoutingElement : \n");
    builder.append("\tFieldName : " + this.fieldName + "\n");
    builder.append("\tDataType : " + this.dataType + "\n");
    builder.append("\troutingRange : \n");
    builder.append("\t\t" + this.routingRange.toString() + " ]\n");
    
    return builder.toString();
    
  }
  
}
