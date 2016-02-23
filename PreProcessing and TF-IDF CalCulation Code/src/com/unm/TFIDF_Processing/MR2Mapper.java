/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/

package com.unm.TFIDF_Processing;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MR2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable Key, Text Value,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		String article= Value.toString();
		String[] splitarticle=article.split("\t");
		String wordcount=splitarticle[1];
		String url= splitarticle[0].split("@")[1];
		String word=splitarticle[0].split("@")[0];
		
		output.collect(new Text(url), new Text(word+"="+wordcount));	//Collecting the Mapper Key,Value(url,word=wordcount) pairs.
				
		}

}
