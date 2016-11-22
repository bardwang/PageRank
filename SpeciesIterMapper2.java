// 
 // Author - Jack Hebert (jhebert@cs.washington.edu) 
 // Copyright 2007 
 // Distributed under GPLv3 
 // 
// Modified - Xun Wang

package hadoop;
 import java.io.IOException;
 import java.util.regex.Matcher;
 import java.util.regex.Pattern; 
  
 import org.apache.hadoop.io.Writable; 
 import org.apache.hadoop.io.WritableComparable; 
 import org.apache.hadoop.mapred.MapReduceBase; 
 import org.apache.hadoop.mapred.Mapper; 
 import org.apache.hadoop.mapred.OutputCollector; 
 import org.apache.hadoop.mapred.Reporter; 
 import org.apache.hadoop.io.Text; 
  
  
 public class SpeciesIterMapper2 extends MapReduceBase implements Mapper<WritableComparable, Writable, Text, Text> { 
  
   public void map(WritableComparable key, Writable value, 
                   OutputCollector output, Reporter reporter) throws IOException { 
  
     // get the current page
     String data = ((Text)value).toString(); 
     int index = -1;
    
     CharSequence inputStr = data;
     String patternStr = "[0-9]+(\\.[0-9]+)?:";
     Pattern pattern = Pattern.compile(patternStr);
     Matcher matcher = pattern.matcher(inputStr);
     if(matcher.find()){
     index = matcher.end() - 1;//this will give you index
     }
     if (index == -1) { 
       return; 
     } 

     // split into title and PR (tab or variable number of blank spaces)
     String toParse = data.substring(0, index).trim(); 
     String[] splits = toParse.split("\t"); 
     if(splits.length == 0) {
       splits = toParse.split(" ");
            if(splits.length == 0) {
               return;
            }
     }
     String pagetitle = splits[0].trim(); 
     String pagerank = splits[splits.length - 1].trim();
     
     // parse current score
     double currScore = 0.0;
     try { 
        currScore = Double.parseDouble(pagerank); 
     } catch (Exception e) { 
        currScore = 1.0;
     } 

     // get number of outlinks
     data = data.substring(index+1);
     data = data.trim();
     String[] pages = data.split(" "); 
     int numoutlinks = 0;
     if (pages.length == 0) {
        numoutlinks = 1;
     } else {
       for (String page : pages) { 
         if(page.length() > 0) {
            numoutlinks = numoutlinks + 1;
         }
       } 
     }

     for (String page : pages) { 
       if(page.length() > 0) {
         page = page.trim(); 
         output.collect(new Text(pagetitle + "-" + page), new Text("")); 
       }
     }
     
     output.collect(new Text(pagetitle), new Text("" + currScore + " " + pages.length));

     // collect the inlink with its dampening factor, and all outlinks
     output.collect(new Text(pagetitle), new Text(".02")); 
     output.collect(new Text(pagetitle), new Text(" " + data)); 
   } 
 } 
 