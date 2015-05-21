package com.hadoop.mapreduce;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class Climatesortcomparator extends WritableComparator{
	
	public Climatesortcomparator(){
		super(FloatWritable.class,true);
	}
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable w1, WritableComparable w2){
		FloatWritable k1 = (FloatWritable) w1;
		FloatWritable k2 = (FloatWritable) w2;
		return -1 * k1.compareTo(k2);
	}
}
