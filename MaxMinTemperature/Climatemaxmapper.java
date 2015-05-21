/**
 * 
 */
package com.hadoop.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Sudheer
 * mapper part of the program
 * takes in object as input key,text as input value and (temperature,village details) 
 * as output key and value
 * 
 */
public class Climatemaxmapper extends Mapper<Object,Text,Text,Text>{
	
	//defining a hash map for aggregation
	 Map<Float, String> tempAggregator = new HashMap<Float, String>();	
	 Map<Float, String> minTempAggregator = new HashMap<Float, String>();
	 String values;
	
	public void map(Object key, Text value,Context context) throws IOException, InterruptedException{
		//getting the required data by splits
		String csvSplit = ",";
		String[] line = value.toString().split(csvSplit);
		try{
		//getting the max temperature from the data
		Float temperature = Float.parseFloat(line[6]);
		//getting the min temperature from the data
		Float minTemperature = Float.parseFloat(line[5]);
		//combination of district+village+state
		values = line[2]+"/"+line[3]+"/"+line[4];
		//aggregator push for max temp
		tempAggregator.put(temperature, values);
		//aggregator push for min temp
		minTempAggregator.put(minTemperature,values);
		}catch(NumberFormatException e){}
		}
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{

		//defining max and min values
		Float max = Float.MIN_VALUE;
		Float min = Float.MAX_VALUE;
		Text minKey = new Text();
		minKey.set("MinTemperature");
		Text maxKey = new Text();
		maxKey.set("MaxTemperature");
		String maxTemp = "";
		String minTemp = "";
		
		//aggregator iteration for max values
		for(Map.Entry<Float, String> entry : tempAggregator.entrySet()){
			max = Math.max(entry.getKey(),max);
		}
		maxTemp = tempAggregator.get(max);
		
		//aggregator iteration for min values
		for(Map.Entry<Float, String> entry : minTempAggregator.entrySet()){
			min = Math.min(entry.getKey(), min);
		}
		minTemp = minTempAggregator.get(min);
		//string conversion
		String maxString = Float.toString(max)+"/"+maxTemp;
		
		//concatenating temperature with district,village and state names
		String minString = Float.toString(min)+"/"+minTemp;
		
		//writing off the key and value pair for max temperature
		context.write(maxKey, new Text(maxString));
		
		//writing off the key and value for min temp
		context.write(minKey,new Text(minString));
	}



}


