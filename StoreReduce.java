package com.top.five.hourly.sarath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StoreReduce
	extends
	Reducer<CompositeKeyWritable, Text, Text,IntWritable> {
	private Map<Text, IntWritable> countMap = new HashMap<>(); 
@Override
public void reduce(CompositeKeyWritable key, Iterable<Text> values,
		Context context) throws IOException, InterruptedException {
	
	 int maxPrice = Integer.MIN_VALUE;
	 
	 int store_id=0;
	 int price=0;
	 int total=0;

//
//     for (Text val : values) {
//    	
//    	 
//    	 String [] valTokens = val.toString().split("\\s+");
//    	 price = Integer.parseInt(valTokens[0]);
//    	
//    	 
//    	 if(price > maxPrice){
//    		 store_id = Integer.parseInt(valTokens[1]);
//    		 maxPrice = price;
//         }
//    	 
//     }
//	
//     context.write(new Text(key.getTstmp()),new Text(maxPrice +"\t"+store_id));
	 
	 for (Text val : values) {
		 String [] valTokens = val.toString().split("\\s+");
    	 price = Integer.parseInt(valTokens[0]);
    	 store_id = Integer.parseInt(valTokens[1]);
    	 countMap.put(new Text(key.getTstmp()+","+store_id), new IntWritable(price)); 	
		}
	 
	 

		
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		
		Map<Text, IntWritable> sortedMap = sortByValues(countMap);

		int counter = 0;

		for (Text key : sortedMap.keySet()) {
			counter++;
			if (counter == 6) {
				break;
			}
			context.write(key, sortedMap.get(key));
		}
	}
	
	public static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(
			Map<K, V> map) {
		List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(
				map.entrySet());
		
		Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

			public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {

			return o2.getValue().compareTo(o1.getValue());
				
			}
		});

		Map<K, V> sortedMap = new LinkedHashMap<K, V>();

		for (Map.Entry<K, V> entry : entries) {
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
}
	