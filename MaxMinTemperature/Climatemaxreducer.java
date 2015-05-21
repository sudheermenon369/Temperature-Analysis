/**
 * 
 */
package com.hadoop.mapreduce;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Sudheer
 * reducer part of the program which gets input("maxtemp",temperatures) from the mapper
 * aggregates the values and maps to a single value which will be pushed to the context
 */
public class Climatemaxreducer extends Reducer<Text,Text,Text,Text>{
	@Override
	public void reduce(Text keys ,Iterable<Text> values, Context context2) throws IOException, InterruptedException{
		
		Text maxKeyString = new Text();
		maxKeyString.set("MaxTemperature");
		
		Text minKeyString = new Text();
		minKeyString.set("MinTemperature");
		//definitions
		Float max = 0F;
		Float maxToFloat = 0F;
		String maxOutputValue = "";
		Float min = Float.MAX_VALUE;
		Float minToFloat = 0F;
		String maxTempOutputValue = "";
		String minTempOutputValue = "";
		String minOutputValue = "";
		for(Text val : values){
			String[] temp = val.toString().split("/");
			try{
				if(keys.equals(maxKeyString)){
					maxToFloat = Float.parseFloat(temp[0]);
					max = Math.max(maxToFloat, max);
					maxTempOutputValue = temp[1]+"/"+temp[2]+"/"+temp[3];
				}
				else if(keys.equals(minKeyString)){
					minToFloat = Float.parseFloat(temp[0]);
					min = Math.min(minToFloat,min);
					minTempOutputValue = temp[1]+"/"+temp[2]+"/"+temp[3];
				}
			}catch(NumberFormatException e){}
 		}
		if(keys.equals(maxKeyString)){
			
			//writing max
			maxOutputValue = max.toString()+"/"+maxTempOutputValue;
			context2.write(keys,new Text(maxOutputValue));
		}
		else if(keys.equals(minKeyString)){
			//writing min
			minOutputValue = min.toString()+"/"+minTempOutputValue;
			context2.write(keys,new Text(minOutputValue));
		}
 	}
}
		

 


