/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/
package com.unm.TFIDF_Processing;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


public class MR2Reducer extends MapReduceBase implements Reducer<Text, Text, Text,Text>{

	@Override
	public void reduce(Text Key, Iterator<Text> Values,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		int sumOfWords=0;
		Map<String,Integer> word_count_map=new HashMap<String, Integer>();
		while(Values.hasNext()){
			String word_count=Values.next().toString();
			word_count_map.put(word_count.split("=")[0],Integer.parseInt(word_count.split("=")[1]));
			sumOfWords+=Integer.parseInt(word_count.split("=")[1]);
		}
		for(String wordtemp:word_count_map.keySet()){	//Collecting Reducer Key,Values(word@url,term frequency) pairs.
			output.collect(new Text(wordtemp+"@"+Key.toString()),new Text(word_count_map.get(wordtemp)+"/"+sumOfWords));  
		}
		
		
	}

}
