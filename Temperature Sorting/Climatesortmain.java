package com.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;

public class Climatesortmain {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		//Initializing a new job
		Job job = Job.getInstance(new Configuration()); 
		
		//Key and value class
		job.setOutputKeyClass(FloatWritable.class);
		job.setOutputValueClass(Text.class);
		
		//Mapper and reducer class
		job.setMapperClass(com.hadoop.mapreduce.Climatesortmapper.class);
		job.setReducerClass(com.hadoop.mapreduce.Climatesortreducer.class);
		
		//Job ip and op class and passing the keys to the comparator
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setSortComparatorClass(Climatesortcomparator.class);
		
		//Ip and op check
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//setting the jar
		job.setJarByClass(Climatesortmain.class);
		job.submit();
		
		System.exit(job.waitForCompletion(true)?0:1);
		}
}
