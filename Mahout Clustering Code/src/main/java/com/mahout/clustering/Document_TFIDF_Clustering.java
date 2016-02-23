/****
 * Author: Harish and Mahesh
 * 
 * 
 * 
 * *****/
package com.mahout.clustering;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.kmeans.Kluster;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class Document_TFIDF_Clustering {
	public static void main(String args[]) throws Exception {

		List<NamedVector> DocumentList = new ArrayList<NamedVector>();
		NamedVector documentVector;
		String line;
		String word_url;
		String TFIDF;	//Reading the Hadoop TF-IDF processed output file 
		BufferedReader br = new BufferedReader(new FileReader("InputData/Challenge1Output.csv"));
		while ((line = br.readLine()) != null) {
			word_url = line.split(",")[0].trim();
			TFIDF = line.split(",")[1].trim();
			//Transforming the Hadoop output data into vectors
			documentVector = new NamedVector(new DenseVector(new double[] { 0.0,Double.valueOf(TFIDF) }), word_url);
			DocumentList.add(documentVector);
		}
		br.close();

		//Creating directory for clustering input data 
		File testData= new File("TextClustering/TextData");
		if (!testData.exists()) {
			testData.mkdir();
		}
		//Creating directory for clusters
		testData = new File("TextClustering/TextClusters");
		if (!testData.exists()) {
			testData.mkdir();
		}
				
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);

		Path path = new Path("TextClustering/TextData/datafile");
		
		//Writing the vectors to sequence files
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path,
				Text.class, VectorWritable.class);
		VectorWritable vec = new VectorWritable();
		for (NamedVector vector : DocumentList) {
			vec.set(vector);
			writer.append(new Text(vector.getName()), vec);
		}
		writer.close();
		
		Path clusterpath = new Path("TextClustering/TextClusters/part-00000");
		
		//Writing the intial cluster centriods to sequence file
		SequenceFile.Writer writer1 = new SequenceFile.Writer(fs, conf,
				clusterpath, Text.class, Kluster.class);

		int k = 18;

		for (int i = 0; i < k; i++) {
			Vector clustervec = DocumentList.get(i);
			Kluster cluster = new Kluster(clustervec, i,new EuclideanDistanceMeasure());
			writer1.append(new Text(cluster.getIdentifier()), cluster);
		}
		writer1.close();

		//Running the Mahout KMeans Algorithm by passing input,cluster,output and other parameters
		KMeansDriver.run(conf, new Path("TextClustering/TextData/datafile"),new Path("TextClustering/TextClusters/part-00000"), new Path(
						"TextClustering/Output"), 0.001, 50, true, 0, true);

		//Reading the Cluster Output formed
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("TextClustering/Output/" + Cluster.CLUSTERED_POINTS_DIR
						+ "/part-m-0"), conf);

		IntWritable key = new IntWritable();
		WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();

		BufferedWriter[] bw = new BufferedWriter[k];
		for (int i = 0; i < k; i++) {
			bw[i] = new BufferedWriter(new FileWriter("Output/Cluster-" + i+".csv"));
		}
		
		//BufferedWriter bw = new BufferedWriter(new FileWriter("Output/Outputtotal.csv"));
		String writeline;

		//Writing the Cluster Output to file
		while (reader.next(key, value)) {
			int clusterNum = Integer.parseInt(key.toString());
			writeline = ((NamedVector) value.getVector()).getName()+ ","+ value.getVector().get(1)+","+clusterNum + "\n";
			bw[clusterNum].write(writeline);

		}
		/*while (reader.next(key, value)) {
			int clusterNum = Integer.parseInt(key.toString());
			writeline = ((NamedVector) value.getVector()).getName()+ ","+ value.getVector().get(1)+","+clusterNum + "\n";
			bw.write(writeline);
		}*/
		for (int i = 0; i < 5; i++) {
			bw[i].close();
		}
		//bw.close();
		reader.close();
		}
}