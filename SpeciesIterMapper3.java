// 
// Modified - Xun Wang
// 

package hadoop;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SpeciesIterMapper3 extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> { 
	  
	   public void map(WritableComparable key, Writable value, 
	                   OutputCollector output, Reporter reporter) throws IOException { 
	  
		   
		   int index = value.toString().indexOf("\t");
		   String fkey = value.toString().substring(0, index);
		   String fvalue = value.toString().substring(index + 1);
		   
		   output.collect(new Text(fkey), new Text(fvalue));
	   } 
	 } 
