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


public class MR3Reducer extends MapReduceBase implements Reducer<Text, Text, Text,Text>{

	@Override
	public void reduce(Text Key, Iterator<Text> Values,
			OutputCollector<Text, Text> output, Reporter arg3)
			throws IOException {
		int WordInNoOfDocs=0;
		int totalNoOfDocs=39809; 
		Map<String,String> url_tf_map=new HashMap<String, String>();
		while(Values.hasNext()){
			String url_tf=Values.next().toString();
			url_tf_map.put(url_tf.split("=")[0],url_tf.split("=")[1]);
			WordInNoOfDocs++;
		}
		double idf=Math.log10(Double.valueOf(Double.valueOf(totalNoOfDocs)/Double.valueOf(WordInNoOfDocs)));	//Calculating IDF
		double tfidf=0.0000;
		for(String url:url_tf_map.keySet()){
			double tf=Double.valueOf(Double.valueOf(url_tf_map.get(url).split("/")[0])/Double.valueOf(url_tf_map.get(url).split("/")[1]));
			tfidf=tf*idf;	//Calculating TF-IDF
			output.collect(new Text(Key.toString()+"@"+url.toString()+","),new Text(String.valueOf(tfidf))); //Collecting Key,Value(word@url,TF-IDF) pairs	
		}
		
	}

}
