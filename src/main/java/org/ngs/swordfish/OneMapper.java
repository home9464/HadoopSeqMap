package org.ngs.swordfish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneMapper extends Mapper<Text, Text, NullWritable, NullWritable>
{
	//public final String user = System.getProperty("user.name");
	// the temporary path on HDFS
	//public final String pathOutputHdfs = String.format("/user/%s/job/output/", user);
	//public final String CpuNum = Integer.toString(Runtime.getRuntime().availableProcessors());
	/**
	 * The key is the full path of command file on DataNode. "/home/hadoop/job/1/A.cmd"
	 * 
	 * */
	public void map(Text key, Text value, Context context) throws IOException 
	{
		
		//Key is the command file Path
		String commandFile = key.toString();
		
		String workingPath  = FilenameUtils.getFullPathNoEndSeparator(commandFile);
		String cmdFileName  = FilenameUtils.getName(commandFile);

		//run the command file as a script
		
		String ret = Util.executeShellStdout(workingPath,cmdFileName);
		
		if (ret.startsWith("Exception") || ret.startsWith("Error")) {
			throw new IOException(ret);
		}
		
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.newInstance(conf);
		
		// copy output file from DataNode's local disk to HDFS
		
		//list all files under working directory
		File[] ffiles = new File(workingPath).listFiles();		
		List<String> allFiles = new ArrayList<>();
		for(File f:ffiles)
		{
			allFiles.add(f.getAbsolutePath());
		}
		
		//list all known input files
		List<String> inputFiles = Arrays.asList(value.toString().split(" "));
		
		//exclude input files from all files, then remaining files are outputs
		allFiles.remove(commandFile);
		allFiles.removeAll(inputFiles);
		
		//copy remaining files to designated HDFS path for this job
		Path dst = new Path(conf.get("JOB_OUTPUT_HDFS_PATH"));
		fs.mkdirs(dst);
		for(String output: allFiles)
		{
			//copy results from DataNode's local disk to HDFS
			fs.copyFromLocalFile(new Path(output), dst);
		}
		//delete results from DataNode's local disk
		//Util.execute(String.format("rm -fr %s",workingPath));
	}
}
