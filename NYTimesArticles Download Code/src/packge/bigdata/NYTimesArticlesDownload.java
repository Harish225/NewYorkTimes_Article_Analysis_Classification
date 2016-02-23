/****
 * 
 * 
 *  Authors:Maheswarareddy Durgempudi and Harish Dara
 * 
 * 
 * Purpose: Main aim of program is to separate URL and Abstract from 50000 articles downloaded form NYTimesTimes Newswire API 
 * 
 */

package packge.bigdata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;


public class NYTimesArticlesDownload {

	public static void main(String[] args) throws IOException, InterruptedException {
	

		File file = new File("50000_Articles.txt"); //Creating file for writing URL and Abstract form Times Newswire API
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		
		 Set<String> lines = new HashSet<>(50000);//Hash set for storing URL for checking duplication of Article duplication
		 
		for(int offset=0;offset<=50000;offset=offset+20)    //loop for downloading 50000 Articles 
		{
			System.out.println("Number of Articles downloaded is:"+offset);
		
		
			try {
                 
				
                String url="http://api.nytimes.com/svc/news/v3/content/all/all.json?limit=20&offset="+offset+"&api-key=a9ac4aa21ef290278f02f452fb41cb2f:4:69919719"; //Declaration of URL
                 
				URL timesNewsWireAPIURL = new URL(url);
				
				int count=0; 

				HttpURLConnection conn = (HttpURLConnection) timesNewsWireAPIURL.openConnection(); //opening connection for given URL 

				if (conn.getResponseCode() != 200) {
					throw new RuntimeException("Failed : HTTP error code : "
							+ conn.getResponseCode());
				}

				BufferedReader reader = new BufferedReader(new InputStreamReader(
					  conn.getInputStream()));                      //Reading  from NewYork Times Wire API URL
				      JsonReader rdr = Json.createReader(reader);
				      JsonObject obj = rdr.readObject();      
                      JsonArray results = obj.getJsonArray("results"); //Creating JSONArray for JSON data.
                      
			    for(JsonObject result : results.getValuesAs(JsonObject.class)) {
				
		      	   String nyTimesURL=result.getString("url");   //Extracting URL from JSON file 
				   
		      	   String nyTimesAbstract=result.getString("abstract"); //Extracting Abstract from JSON file 
				   
				                    
				       if(lines.add(nyTimesURL))  // Control flow Statement for Checking duplicate articles
				          {
				
				               writer.write(nyTimesURL);  //Writing URL to file
				               writer.write("::::");
				               writer.write(nyTimesAbstract);//Writing Abstract to file
					            writer.write("\n");
				            }
				      else
				       {
					   
				    	count++; 
					   System.out.println("Duplicate Article found at "+count+" URL : "+nyTimesURL);
				   }
				   
			}				     
				
				
	           }

			catch (Exception e)              // Catch statement for handling Exception 
			{
				System.out.println("Exception : " + e.getMessage());
				Thread.sleep(1000);           //Giving a delay of 1000ms for next try when Exception is thrown 
				continue;
			}

		}
		writer.close();
	}
}
