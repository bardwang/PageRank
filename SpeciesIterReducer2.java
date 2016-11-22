// 
// Modified - Xun Wang
// 

package hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SpeciesIterReducer2 extends MapReduceBase implements Reducer<WritableComparable, Text, Text, Text> { 
	  
	   String fkey = "";
	   double p = 0.0;
	   int numoutlinks = 1;
	   
	   public void reduce(WritableComparable key, Iterator values, 
	                      OutputCollector output, Reporter reporter) throws IOException { 

		 if(key.toString().contains("-")){
			 String key1 = key.toString();
			 int a = key1.indexOf("-");
			 fkey = key1.substring(a+1);
			 double finalnum = 0.98 * p / numoutlinks;
		     output.collect(fkey, new Text("" + finalnum));
	     }else{
	     while (values.hasNext()) { 
	       String value = values.next().toString();
	       String[] vs = value.split(" ");
	       if(vs.length == 2){
	    	   try{
	    	   double num1 = Double.parseDouble(vs[0]);
	    	   int num2 = Integer.parseInt(vs[1]);
	    	   p = num1;
	    	   numoutlinks = num2;
	    	   }catch (Exception e){
	    	   output.collect(key, value);
	    	   }
	       }else{
	    	   output.collect(key, value);
	       }
	     }
	     
	     }

		  
	   } 
	 } 