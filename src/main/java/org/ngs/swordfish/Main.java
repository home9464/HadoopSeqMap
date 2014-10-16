package org.ngs.swordfish;
import java.io.IOException;
import java.nio.file.Files;

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

public class Main
{
	public Main()
	{
		
	}
	
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
		int numCpuCoresPerContainer = ClusterStats.getNumCpuCoreDN() / numContainersPerNode;
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
		conf.setInt("yarn.scheduler.minimum-allocation-mb", (int) (memoryMbPerContainer));

		// CPU, max/mim allocation for every container request at the RM
		conf.setInt("yarn.scheduler.maximum-allocation-vcores", ClusterStats.getNumCpuCoreDN());
		conf.setInt("yarn.scheduler.minimum-allocation-vcores", numCpuCoresPerContainer);

		// Memory, max/mim allocation for every container request at the RM
		conf.setInt("mapreduce.map.memory.mb", memoryMbPerContainer);
		conf.setInt("mapreduce.reduce.memory.mb", memoryMbAvailable);

		conf.setInt("mapreduce.map.java.opts':'-Xmx%dm",mapreduce_map_java_opts);
		conf.setInt("mapreduce.reduce.java.opts':'-Xmx%dm",mapreduce_reduce_java_opts);
		
	}
	public static void main(String[] args)
	{
		
		Option i = OptionBuilder.withArgName( "input file or directory" ).hasArg().isRequired()
				.withDescription("Inputs for this job" ).create( "i" );

		Option o = OptionBuilder.withArgName( "output directory" ).hasArg().isRequired() 
				.withDescription("Output folder for this job" ).create( "o" );


		Options options = new Options();
		options.addOption(i);
		options.addOption(o);
		CommandLineParser parser = new BasicParser();

		String localInputPath = ".";
		String localOutputPath = ".";
		String jobPath = null;

		String currentDirectory = System.getProperty("user.dir");
		// "$HOME/A/B/C" -> "/A/B/C" 
		String strippedDirectory = currentDirectory.replaceFirst(System.getProperty("user.home"),"");
		
		String hdfsInputPath = "/user/hadoop"+strippedDirectory+"/input";
		String hdfsOutputPath = "/user/hadoop"+strippedDirectory+"/output"; 
		String hdfsTmpPath = "/user/hadoop"+strippedDirectory+"/tmp"; 

		//Path hdfsInputPath = new Path("/user/hadoop"+strippedDirectory+"/input"); 
		//Path hdfsOutputPath = new Path("/user/hadoop"+strippedDirectory+"/output"); 
		//Path hdfsTmpPath =new Path("/user/hadoop"+strippedDirectory+"/tmp"); 
		CommandLine line;
		Configuration conf = new Configuration();
		configureHadoop(conf,1);
		FileSystem fs=null;
		
		try
		{
			line = parser.parse(options, args);
			localInputPath = line.getOptionValue("i");
			localOutputPath = line.getOptionValue("o");
					
			fs = FileSystem.newInstance(conf);
			fs.copyFromLocalFile(new Path(localInputPath), new Path(hdfsInputPath));

			//TODO: no need for the splitted files on local disk
			fs.delete(new Path(localInputPath),true);

			Job job = Job.getInstance(conf, "bwa");
			job.setNumReduceTasks(0);
			job.setJarByClass(Main.class);

			job.setInputFormatClass(CommandFileInputFormat.class);
			// job.setOutputFormatClass(NullOutputFormat.class);

			job.setMapperClass(OneMapper.class);
			job.setReducerClass(NoOutputReducer.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.addInputPath(job,new Path(hdfsInputPath));
			FileOutputFormat.setOutputPath(job, new Path(hdfsTmpPath));

			job.waitForCompletion(true);
		
			//Local directory will be created automatically if not exist
			fs.copyToLocalFile(new Path(hdfsOutputPath),new Path(currentDirectory,localOutputPath));
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		finally
		{
			try 
			{
				fs.delete(new Path(hdfsInputPath), true);
				fs.delete(new Path(hdfsOutputPath), true);
				fs.delete(new Path(hdfsTmpPath), true);
				
			} catch (IllegalArgumentException | IOException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
