/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/

package com.unm.TFIDF_Processing;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MR2Driver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if(args.length<2){
			System.out.println("Please Enter Required number of Directories");
			return -1;
		}
		
		JobConf conf= new JobConf(MR2Driver.class);
		conf.setJobName("Challenge-1Job");
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf,new Path(args[1]));
		
		//Setting Mapper and Reducer Classes for MR2 	
		conf.setMapperClass(MR2Mapper.class);
		conf.setReducerClass(MR2Reducer.class);
		
		//Setting Class types for Key,Value Mapper Outputs
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);
		
		//Setting Class types for Key,Value Reducer Outputs
		conf.setOutputKeyClass(Text.class);
		conf.setOutputKeyClass(Text.class);
		
		JobClient.runJob(conf);
	
		return 0;
	}
	public static void main(String[] args) throws Exception{
		int exitcode=ToolRunner.run(new MR2Driver(),args);
		System.exit(exitcode);
	}
}
