/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/

package com.unm.TFIDF_Processing;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MR1Reducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{

	@Override
	public void reduce(Text Key, Iterator<IntWritable> Value,
			OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		int sum=0;
		while(Value.hasNext()){
			sum+=Value.next().get();
		}
		output.collect(Key,new IntWritable(sum));  //Collecting the Reducer Key,Value(word@url,word count in document).
		
	}

}
