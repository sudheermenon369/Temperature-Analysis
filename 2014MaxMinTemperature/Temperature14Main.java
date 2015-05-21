/**
 * 
 */
package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Sudheer
 *
 */
public class Temperature14Main {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		//Initializing a new job
				Job job = Job.getInstance(new Configuration()); 
				
				//Key and value class
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				
				//Mapper and reducer class
				job.setMapperClass(com.hadoop.mapreduce.Temperature14Mapper.class);
				job.setReducerClass(com.hadoop.mapreduce.Temperature14Reducer.class);
				
				//Job ip and op class
				job.setInputFormatClass(TextInputFormat.class);
				job.setOutputFormatClass(TextOutputFormat.class);
				
				//Ip and op check
				FileInputFormat.setInputPaths(job, new Path(args[0]));
				FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
				//setting the jar
				job.setJarByClass(Temperature14Main.class);
				job.submit();
				
				System.exit(job.waitForCompletion(true)?0:1);


	}

}