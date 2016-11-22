// 
// Author - Jack Hebert (jhebert@cs.washington.edu) 
// Copyright 2007 
// Distributed under GPLv3 
// 
// Modified - Xun Wang
// 
package hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;

import hadoop2.SpeciesViewerDriver;
import hadoop3.SpeciesGraphBuilder;

import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;

public class SpeciesDriver {

	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Usage: PageRankIter <input path> <num_round>");
			System.exit(0);
		}
		
		// Graph builder
		 JobClient client = new JobClient(); 
	     JobConf confg = new JobConf(SpeciesDriver.class); 
	     confg.setJobName("Page-rank Species Graph Builder");
	   
	     confg.setMapperClass(SpeciesGraphBuilderMapper.class); 
	     confg.setMapOutputKeyClass(Text.class);
	     confg.setMapOutputValueClass(Text.class);

	     confg.setReducerClass(SpeciesGraphBuilderReducer.class); 
	     FileInputFormat.setInputPaths(confg, new Path(args[0]));
	     FileOutputFormat.setOutputPath(confg, new Path("output"));  
	  
	     client.setConf(confg); 
	     try { 
	       JobClient.runJob(confg); 
	     } catch (Exception e) { 
	       e.printStackTrace(); 
	     }

	    // Iteration
		String input = "output";
		int NUM_ROUNDS = Integer.parseInt(args[1]);
		String output = "output";

		for (int i = 1; i < NUM_ROUNDS * 2; i++) {

			JobConf conf = new JobConf(SpeciesDriver.class);
			conf.setJobName("Species Iter 1");
			// conf.setNumReduceTasks(5);
			conf.setOutputKeyClass(Text.class);
			conf.setOutputValueClass(Text.class);

			String tempoutput = "output" + i;
			FileInputFormat.setInputPaths(conf, new Path(input));
			FileOutputFormat.setOutputPath(conf, new Path(tempoutput));

			conf.setMapperClass(SpeciesIterMapper2.class);
			conf.setReducerClass(SpeciesIterReducer2.class);

			JobConf conf2 = new JobConf(SpeciesDriver.class);
			conf2.setJobName("Species Iter 2");
			// conf2.setNumReduceTasks(5);
			conf2.setOutputKeyClass(Text.class);
			conf2.setOutputValueClass(Text.class);

			conf2.setMapperClass(SpeciesIterMapper3.class);
			conf2.setReducerClass(SpeciesIterReducer3.class);

			FileInputFormat.setInputPaths(conf2, new Path(tempoutput));
			i++;
			output = "output" + i;
			FileOutputFormat.setOutputPath(conf2, new Path(output));

			client.setConf(conf);
			try {
				JobClient.runJob(conf);
			} catch (Exception e) {
				e.printStackTrace();
			}

			client.setConf(conf2);
			try {
				JobClient.runJob(conf2);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			input = output;
		}
		
		// Graph viewer 
	     JobConf confv = new JobConf(SpeciesDriver.class); 
	     confv.setJobName("Species Viewer");
	     confv.setOutputKeyClass(FloatWritable.class); 
	     confv.setOutputValueClass(Text.class); 
	     FileInputFormat.setInputPaths(confv, new Path(output));
	     FileOutputFormat.setOutputPath(confv, new Path("finaloutput"));
	  
	     confv.setMapperClass(SpeciesViewerMapper.class); 
	     confv.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class); 
	  
	     client.setConf(confv); 
	     try { 
	       JobClient.runJob(confv); 
	     } catch (Exception e) { 
	       e.printStackTrace(); 
	     }
	     
	}
}
