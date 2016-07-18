package com.top.five.hourly.sarath;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

//public class StoresMap extends Mapper <LongWritable,Text,Text,IntWritable> {
public class StoresMap extends Mapper <LongWritable,Text,CompositeKeyWritable,Text> {

	private Text tstmp = new Text();
	private Text store_no = new Text();
	private Text price = new Text();
	
	public void map (LongWritable Key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String line = value.toString();
		String [] ar = line.split(",");
		
		tstmp.set(ar[3].substring(11, 13));
		store_no.set(ar[0]);
		price.set(ar[2]);
		//context.write(new Text(store_no), new IntWritable(Integer.parseInt(price.toString()))); 
		//context.write(new CompositeKeyWritable(ar[3].substring(11, 13).toString()), arg1);

		context.write(
				new CompositeKeyWritable(tstmp.toString(),
						store_no.toString()),
						new Text(price+"\t"+store_no));

	}

}
