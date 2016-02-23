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


public class MR3Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

	@Override
	public void map(LongWritable Key, Text Value,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		String article= Value.toString();
		String[] word_url_tf=article.split("\t");
		String[] word_url=word_url_tf[0].split("@");
		String word= word_url[0];
		String url= word_url[1];
		String tf=word_url_tf[1];
		
		output.collect(new Text(word), new Text(url+"="+tf));	//Collecting Mapper Key,Value(word,url=term frequency)
				
		}

}
