package SamsungStorm.Bolts;

import java.io.File;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class Format {
  public static final String DEFAULTINPUTPATH = "inputschema_.xml";
  public static final String DEFAULTROUTINGPATH = "routingschema.xml";
  
  private String inputPath  = DEFAULTINPUTPATH;
  private String routingPath = DEFAULTROUTINGPATH;
  private HashMap<String , String[]> schemaMap;
  private HashMap<String , String[]> routingMap;
  
  private static DocumentBuilderFactory factory =
      DocumentBuilderFactory.newInstance();
  private Document document;
  
  public Format() throws Exception{
    init();
  }
  public Format(String inputPath , String routingPath) 
      throws Exception{
    this.inputPath = inputPath;
    this.routingPath = routingPath;
    init();
  }
  private void init() throws Exception{
    schemaMap = new HashMap<String , String[]>();
    routingMap = new HashMap<String , String[]>();
    
    document = factory.newDocumentBuilder().parse(new File(inputPath));
    NodeList schemaList = document.getElementsByTagName("schema");
    for(int i = 0 ; i < schemaList.getLength() ; i ++) {
      Element element = (Element)schemaList.item(i);
      NodeList tagName = element.getElementsByTagName("name");
      NodeList range = element.getElementsByTagName("range");
      
      if(tagName.getLength() != 1 || range.getLength() < 1) {
        throw new Exception("Wrong schema format");
      }
      
      String name = tagName.item(0).getTextContent();
      System.out.println("name : " + name);
      String[] rangeArr = new String[range.getLength()];
      System.out.println(range.getLength());
      for(int j = 0 ; j < rangeArr.length ; j ++) {
        System.out.println(range.item(j).getTextContent());
        rangeArr[j] = new String(range.item(j).getTextContent());
      }
      
      this.schemaMap.put(name, rangeArr);
    }
    
    document = factory.newDocumentBuilder().parse(new File(routingPath));
    NodeList routingList = document.getElementsByTagName("schema");
    for(int i = 0 ; i < routingList.getLength() ; i ++) {
      String routingTag = routingList.item(i).getTextContent();
      String[] rangeArr = schemaMap.get(routingTag);
      this.routingMap.put(routingTag, rangeArr);
    }
    
      
  }
  
  public HashMap<String , String[]> getInputSchema() {
    return this.schemaMap;
  }
  
  public HashMap<String , String[]> getRoutingSchema() {
    return this.routingMap;
  }
  
  public static void main(String args[]) throws Exception{
    Format format = new Format();
    System.out.println(format.getInputSchema());
    System.out.println(format.getRoutingSchema());
  }
}
