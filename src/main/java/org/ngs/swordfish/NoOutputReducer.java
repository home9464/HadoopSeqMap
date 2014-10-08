package org.ngs.swordfish;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;



public class NoOutputReducer extends Reducer<NullWritable, NullWritable, NullWritable, NullWritable>
{
	@Override
	public void setup(Context context) 
			throws IOException,InterruptedException
	{

	}

	public void reduce(NullWritable key, Iterable<NullWritable> values,Context context) 
			throws IOException, InterruptedException
	{
		throw new IOException("Not implemented");
	}
}
