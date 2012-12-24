package SamsungStorm.Bolts;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;


public class RoutingSchema {
  public static final String DefaultPath = "conf/inputschema.xml";
  
  private static RoutingSchema routingSchema = null;
  private HashMap<String , RoutingElement> routingElementMap;
  private static DocumentBuilderFactory factory =
      DocumentBuilderFactory.newInstance();
  private Document document;
  
  public static RoutingSchema getInstance(String path) 
      throws IOException, SAXException, ParserConfigurationException{
    if(routingSchema == null) {
      routingSchema = new RoutingSchema(path);
    } 
    return routingSchema;
  }
 
  
  private RoutingSchema(String xmlFilePath) 
      throws IOException, SAXException, ParserConfigurationException {
    routingElementMap = new HashMap<String , RoutingElement>();
//    document = factory.newDocumentBuilder().parse(new File(xmlFilePath));
    String s = "<routingschema>  <routingelement>    <fieldname>category</fieldname>    <datatype>string</datatype>    <datarange>      <rangeelement>eat</rangeelement>      <rangeelement>playing</rangeelement>    </datarange>  </routingelement>  <routingelement>    <fieldname>age</fieldname>    <datatype>int</datatype>    <datarange>      <rangeelement>10</rangeelement>      <rangeelement>20</rangeelement>      <rangeelement>30</rangeelement>      <rangeelement>40</rangeelement>    </datarange>  </routingelement></routingschema>";
    ByteArrayInputStream is = new ByteArrayInputStream(s.getBytes());
    document = factory.newDocumentBuilder().parse(is);
    
    //get each routing element
    NodeList routingList = document.
        getElementsByTagName(Constants.RoutingSchema.routingElement);
    
    for(int i = 0 ; i < routingList.getLength() ; i ++) {
      
      Element element = (Element) routingList.item(i);
      NodeList nodeInfos = element.getChildNodes();
      String fieldName = null;
      String dataType = null;
      ArrayList<String> dataRange = null;
      
      for(int j = 0 ; j < nodeInfos.getLength() ; j ++ ) {
        Node node = nodeInfos.item(j);
        
        String nodeName = node.getNodeName();
        if(nodeName.equals("#text")) {
          continue;
        }else if(nodeName.equals(Constants.RoutingSchema.dataType)) {
          System.out.println("DataType : " + node.getTextContent());
          dataType = node.getTextContent();
        }else if(nodeName.equals(Constants.RoutingSchema.fieldName)) {
          System.out.println("FieldName : " + node.getTextContent());
          fieldName = node.getTextContent();
        }else if(nodeName.equals(Constants.RoutingSchema.dataRange)) {
          System.out.println("DataRange : " + node.getTextContent());
          NodeList list = ((Element)node).
              getElementsByTagName(Constants.RoutingSchema.rangeElement);
          dataRange = new ArrayList<String>(list.getLength());
          for(int k = 0 ; k < list.getLength() ; k ++ ) {
            dataRange.add(list.item(k).getTextContent());
          }
        }
        
      }
      //validation check
      if(fieldName == null || dataType == null || dataRange == null) {
        throw new ParserConfigurationException(
            "Routing Schema is not loaded");
      }
      //put routingelement into map
      this.routingElementMap.put(fieldName, 
          new RoutingElement(fieldName , dataType , dataRange));
    }
    
  }
  
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    for(RoutingElement re : this.routingElementMap.values()) {
      builder.append(re.toString());
      builder.append("\n");
    }
    
    return builder.toString();
  }
  
  public static void main(String [] args) 
      throws IOException, SAXException, ParserConfigurationException {
    RoutingSchema rs = RoutingSchema.getInstance(DefaultPath);
    System.out.println(rs.toString());
  }
}
