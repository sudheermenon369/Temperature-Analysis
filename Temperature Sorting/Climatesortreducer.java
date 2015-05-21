package com.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Climatesortreducer extends Reducer<FloatWritable,Text,FloatWritable,Text>{
	public void reduce(FloatWritable keys ,Text values, Context context2) throws IOException, InterruptedException{
		//pushing through the key and value no alter
		context2.write(keys, values);
		
	}
}
	
