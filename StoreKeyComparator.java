package com.top.five.hourly.sarath;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class StoreKeyComparator extends WritableComparator {
	
	public StoreKeyComparator() {
		super(CompositeKeyWritable.class,true);
	}
	
	public int compare(WritableComparable w1, WritableComparable w2)
	{
		CompositeKeyWritable key1 = (CompositeKeyWritable) w1;
		CompositeKeyWritable key2 = (CompositeKeyWritable) w2;
		
		int comp_result = key1.getTstmp().compareTo(key2.getTstmp());
		if (comp_result == 0)
		{
			return -key1.getStore_id().compareTo(key2.getStore_id());
		}
		
		return comp_result;
	}

}
