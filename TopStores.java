package com.top.five.hourly.sarath;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopStores {
	
	public static void main(String[] args) throws Exception {
		
		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(TopStores.class);	
		job.setMapperClass(StoresMap.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(Text.class);
	    job.setCombinerClass(StoreCombiner.class);
		job.setPartitionerClass(StorePartitioner.class);
		job.setSortComparatorClass(StoreKeyComparator.class);
		job.setGroupingComparatorClass(StoreKeyGroupPartitioner.class);
		job.setReducerClass(StoreReduce.class);	
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean status = job.waitForCompletion(true);
		if (status) {
			System.exit(0);;
		}
		else {
			System.exit(1);
		}
	
				
	}

}
