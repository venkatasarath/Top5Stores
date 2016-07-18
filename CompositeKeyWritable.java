package com.top.five.hourly.sarath;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


public class CompositeKeyWritable implements Writable,
WritableComparable<CompositeKeyWritable> {

private String tstmp;
private String store_id;

public CompositeKeyWritable() {
	
}

public CompositeKeyWritable(String tstmp, String store_id) {
	this.tstmp = tstmp;
	this.store_id = store_id;
}

public String toString() {
	return (new StringBuilder().append(tstmp).append("\t").append(store_id)).toString();
}

public void readFields(DataInput dataInput) throws IOException {
	
	tstmp = WritableUtils.readString(dataInput);
	store_id = WritableUtils.readString(dataInput);
}
public void write(DataOutput dataOutput) throws IOException {
	
	WritableUtils.writeString(dataOutput,tstmp);
	WritableUtils.writeString(dataOutput, store_id);
}

public int compareTo(CompositeKeyWritable objKeyPair){
	
		int result = tstmp.compareTo(objKeyPair.tstmp);
		if(0 == result) {
			result = store_id.compareTo(objKeyPair.store_id);
		}
		return result;
}

public String getTstmp() {
	return tstmp;
}

public void setTstmp(String tstmp){
	this.tstmp=tstmp;
}

public String getStore_id() {
	return store_id;
}

public void setStore_id(String store_id){
	this.store_id=store_id;
}
}
