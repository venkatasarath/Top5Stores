package com.top.five.hourly.sarath;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StoreKeyGroupPartitioner extends WritableComparator{
	
	protected StoreKeyGroupPartitioner()
	{
		super(CompositeKeyWritable.class,true);
	
	}
	
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		CompositeKeyWritable k1 = (CompositeKeyWritable)w1;
		CompositeKeyWritable k2 = (CompositeKeyWritable )w2;
		return k1.getTstmp().compareTo(k2.getTstmp());	
	}

}	
