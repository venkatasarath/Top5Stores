package com.top.five.hourly.sarath;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.Text;


public class StorePartitioner extends Partitioner<CompositeKeyWritable,Text> {
	
		public int getPartition(CompositeKeyWritable Key, Text values, int numReduceTasks)
		{
			return (Key.getTstmp().hashCode() % numReduceTasks);	
		}

}
