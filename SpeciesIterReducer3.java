// 
 // Author - Jack Hebert (jhebert@cs.washington.edu) 
 // Copyright 2007 
 // Distributed under GPLv3 
 // 
// Modified - Xun Wang

package hadoop;

import java.io.IOException; 
 import java.util.Iterator; 
  
 import org.apache.hadoop.io.WritableComparable; 
 import org.apache.hadoop.mapred.MapReduceBase; 
 import org.apache.hadoop.mapred.OutputCollector; 
 import org.apache.hadoop.mapred.Reducer; 
 import org.apache.hadoop.mapred.Reporter; 
 import org.apache.hadoop.io.Text; 
  
 public class SpeciesIterReducer3 extends MapReduceBase implements Reducer<WritableComparable, Text, Text, Text> { 
  
   public void reduce(WritableComparable key, Iterator values, 
                      OutputCollector output, Reporter reporter) throws IOException { 
  
	 double total = 0;
	 String outlinks = "";
	   
     while (values.hasNext()) { 
      String value = values.next().toString();
      try{
      double d = Double.parseDouble(value);
      total = total + d;
      }catch (Exception e){
      outlinks = value;
      }
     } 
  
     output.collect(key, new Text("" + total + ": " + outlinks)); 
   } 
 } 