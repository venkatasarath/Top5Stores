package com.top.five.hourly.sarath;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class StoreCombiner
	extends
	Reducer<CompositeKeyWritable, Text, CompositeKeyWritable, Text> {

@Override
public void reduce(CompositeKeyWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	int store_id =0;
	int price =0;
	 int total = 0;
     for (Text val : values) {
    	 String [] valTokens = val.toString().split("\\s+");
    	 store_id = Integer.parseInt(valTokens[1]);
    	 price = Integer.parseInt(valTokens[0]);
    	 total += price;
     }
     context.write(key, new Text(total+"\t"+store_id));
	}

}

