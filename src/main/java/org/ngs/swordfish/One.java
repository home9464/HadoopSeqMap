package org.ngs.swordfish;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class One
{
	public static void configureHadoop(Configuration conf,int numContainersPerNode)
	{
		float ratio = 0.9f;

		long task_timeout_millsec = 259200000l; // 72 hours
		
		int memoryMbAvailable = (int) (ClusterStats.getMemoryMbDN() * ratio);
		System.out.println("Memory:"+String.valueOf(memoryMbAvailable)+"MB,"+"CPU:"+ClusterStats.getNumCpuCoreDN());
		// int numContainersPerNode = memoryMbAvailable /
		// minMemoryMbPerContainer;

		// if (numContainersPerNode < 1)
		// {
		// throw new Exception(String.format(
		// "Insufficient memory: Asked: %d, Available: %d",
		// minMemoryMbPerContainer, memoryMbAvailable));
		// }

		int numCpuCoresPerContainer = ClusterStats.getNumCpuCoreDN()
				/ numContainersPerNode;
		int memoryMbPerContainer = memoryMbAvailable / numContainersPerNode;
		int mapreduce_map_java_opts = (int) (memoryMbPerContainer * ratio);
		int mapreduce_reduce_java_opts = (int) (memoryMbPerContainer * ratio);

		// http://docs.hortonworks.com/HDPDocuments/HDP2/HDP-2.0.6.0/bk_installing_manually_book/content/rpm-chap1-11.html

		conf.setLong("mapreduce.task.timeout", task_timeout_millsec);

		// Amount of total physical memory in a DataNode that can be allocated
		// for containers.
		conf.setInt("yarn.nodemanager.resource.memory-mb", memoryMbAvailable);

		// Memory, max/mim allocation for every container request at the RM
		conf.setInt("yarn.scheduler.maximum-allocation-mb", memoryMbAvailable);
		conf.setInt("yarn.scheduler.minimum-allocation-mb",
				(int) (memoryMbPerContainer));

		// CPU, max/mim allocation for every container request at the RM
		conf.setInt("yarn.scheduler.maximum-allocation-vcores",
				ClusterStats.getNumCpuCoreDN());
		conf.setInt("yarn.scheduler.minimum-allocation-vcores",
				numCpuCoresPerContainer);

		// Memory, max/mim allocation for every container request at the RM
		conf.setInt("mapreduce.map.memory.mb", memoryMbPerContainer);
		conf.setInt("mapreduce.reduce.memory.mb", memoryMbAvailable);

		conf.setInt("mapreduce.map.java.opts':'-Xmx%dm",
				mapreduce_map_java_opts);
		conf.setInt("mapreduce.reduce.java.opts':'-Xmx%dm",
				mapreduce_reduce_java_opts);
		
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, ParseException 
	{
		
		Option i = OptionBuilder.withArgName( "input file or directory" )
				.hasArg()
				.isRequired() 
				.withDescription("Inputs for this job" )
				.create( "i" );

		Option o = OptionBuilder.withArgName( "output directory" )
				.hasArg()
				.isRequired() 
				.withDescription("Output folder for this job" )
				.create( "o" );


		Options options = new Options();
		options.addOption(i);
		options.addOption(o);
		CommandLineParser parser = new BasicParser();

		String pathInput = ".";
		String pathOutput = ".";
		String jobPath = null;
		CommandLine line;
		line = parser.parse(options, args);

		pathInput = line.getOptionValue("i");
		pathOutput = line.getOptionValue("o");
		
		String currentDirectory = System.getProperty("user.dir");
		String strippedDirectory = currentDirectory.replaceFirst(System.getProperty("user.home"),"");
		
		Path hdfsInputPath = new Path("/user/hadoop"+strippedDirectory+"/input"); 
		Path hdfsOutputPath = new Path("/user/hadoop"+strippedDirectory+"/output"); 
		Path hdfsTmpPath =new Path("/user/hadoop"+strippedDirectory+"/tmp"); 
		
		Configuration conf = new Configuration();
		conf.set("JOB_WORKING_LOCAL_PATH","job/tests");
		conf.set("JOB_OUTPUT_HDFS_PATH","/user/hadoop/tests/output");
		//long task_timeout_millsec = 259200000l; // 72 hours
		//conf.setLong("mapreduce.task.timeout", task_timeout_millsec);
		configureHadoop(conf,1);
		
		FileSystem fs = FileSystem.newInstance(conf);
		fs.delete(hdfsOutputPath, true);
		fs.delete(hdfsTmpPath, true);
		
		fs.copyFromLocalFile(new Path(pathInput), hdfsInputPath);
		
		Job job = Job.getInstance(conf, "bwa");
		job.setNumReduceTasks(0);
		job.setJarByClass(One.class);

		job.setInputFormatClass(CommandFileInputFormat.class);
		// job.setOutputFormatClass(NullOutputFormat.class);

		job.setMapperClass(OneMapper.class);
		job.setReducerClass(NoOutputReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.addInputPath(job,hdfsInputPath );
		FileOutputFormat.setOutputPath(job, hdfsTmpPath);

		job.waitForCompletion(true);
		
		//Local directory will be created automatically if not exist
		fs.copyToLocalFile(hdfsOutputPath,new Path(currentDirectory,pathOutput));
		
		//fs.delete(hdfsOutputPath, true);
		//fs.delete(hdfsTmpPath, true);
	}
}
