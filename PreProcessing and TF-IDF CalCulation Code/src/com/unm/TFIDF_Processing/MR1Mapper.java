/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/

package com.unm.TFIDF_Processing;
import com.unm.wordStemming.Stem;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class MR1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

public static HashSet<String> stopWordsSet= new HashSet<String>();

public static void readStopWords() {
			 
		String csvFile = "/home/harish225/workspace/Challenge1/InputData/stopwords.csv";
		BufferedReader br = null;
		String line;
		String[] wordlist; 
			 
		try {
			 
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null)
				{
					wordlist=line.split(",");
					System.out.println(wordlist);
					Collections.addAll(stopWordsSet, wordlist);
				} 
			
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (br != null) {
				try {
						br.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					}
				}
 
	  }

	  public static boolean isContains(String wordToFind){
				  
			if(stopWordsSet.contains(wordToFind)){
				return true;
			}
			return false;	  
			}	
	
	@Override
	public void map(LongWritable Key, Text Value,OutputCollector<Text, IntWritable> output, Reporter arg3)
			throws IOException {
		
		readStopWords();	//Function to read stop-words from CSV file and store it in a HashSet
		
		String article= Value.toString().trim();
		String delimiter="::::";
		
		if(article.contains(delimiter)){
		
		String[] splitarticle=article.split(delimiter);
		
		String url= splitarticle[0];
		for(int i=1;i<splitarticle.length;i++)
		{
		splitarticle[i]=splitarticle[i].replaceAll("[^A-Za-z ]", "");  //Cleaning the abstract by removing special characters and digits
		splitarticle[i]=splitarticle[i].trim().toLowerCase();
			
			for(String word:splitarticle[i].split(" ")){
				if(word.length()>0){	
					if(!isContains(word)){		//Checking if the word is stop-word or not
						try {
							word=Stem.stemWord(word);		//Stemming the word
							output.collect(new Text(word+"@"+url), new IntWritable(1));		//Collecting Mapper Key,Value(word@url,1)
							} catch (Throwable e) {
								System.out.println("Exception thrown in Word Stemming");
								e.printStackTrace();
						}
					
					}
				}
			}
		}
	}
 }

}
