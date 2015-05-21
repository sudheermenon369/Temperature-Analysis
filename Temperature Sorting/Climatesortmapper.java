package com.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Climatesortmapper extends Mapper<Object,Text,FloatWritable,Text>{
		public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
			//getting the required data by splits
			String csvSplit = ",";
			String[] line = value.toString().split(csvSplit);
			//getting the max temperature from the data
			Float temperature = Float.parseFloat(line[6]);
			//combination of district+village+state
			String values = line[2]+"/"+line[3]+"/"+line[4];
			//writing off the key and value pair
			context.write(new FloatWritable(temperature), new Text(values));
			}
		}

