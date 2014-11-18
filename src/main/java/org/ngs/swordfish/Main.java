package org.ngs.swordfish;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

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
		
		//System.out.println("Memory:"+String.valueOf(memoryMbAvailable)+"MB,"+"CPU:"+ClusterStats.getNumCpuCoreDN());
		
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
		/*
		conf.setInt("yarn.nodemanager.resource.memory-mb", memoryMbAvailable);
		System.out.println("yarn.nodemanager.resource.memory-mb:"+String.valueOf(memoryMbAvailable));

		// Memory, max/min allocation for every container request at the RM
		conf.setInt("yarn.scheduler.maximum-allocation-mb", memoryMbAvailable);
		conf.setInt("yarn.scheduler.minimum-allocation-mb", (int) (memoryMbPerContainer));
		
		System.out.println("yarn.scheduler.maximum-allocation-mb:"+String.valueOf(memoryMbAvailable));
		System.out.println("yarn.scheduler.minimum-allocation-mb:"+String.valueOf((int) (memoryMbPerContainer)));
		

		// CPU, max/mim allocation for every container request at the RM
		conf.setInt("yarn.scheduler.maximum-allocation-vcores", ClusterStats.getNumCpuCoreDN());
		conf.setInt("yarn.scheduler.minimum-allocation-vcores", numCpuCoresPerContainer);
		
		System.out.println("yarn.scheduler.maximum-allocation-vcores:"+String.valueOf(ClusterStats.getNumCpuCoreDN()));
		System.out.println("yarn.scheduler.minimum-allocation-vcores:"+String.valueOf((int) (numCpuCoresPerContainer)));

		// Memory, max/mim allocation for every container request at the RM
		conf.setInt("mapreduce.map.memory.mb", memoryMbPerContainer);
		conf.setInt("mapreduce.reduce.memory.mb", memoryMbAvailable);
		System.out.println("mapreduce.map.memory.mb:"+String.valueOf(memoryMbPerContainer));
		System.out.println("mapreduce.reduce.memory.mb:"+String.valueOf(memoryMbAvailable));

		
		conf.setInt("mapreduce.map.java.opts':'-Xmx%dm",mapreduce_map_java_opts);
		conf.setInt("mapreduce.reduce.java.opts':'-Xmx%dm",mapreduce_reduce_java_opts);
		*/
		
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

		String currentDirectory = System.getProperty("user.dir");
		// "$HOME/A/B/C" -> "/A/B/C" 
		String strippedDirectory = currentDirectory.replaceFirst(System.getProperty("user.home"),"");
		
		String hdfsHome = String.format("/user/%s",System.getProperty("user.name"));
		
		String hdfsInputPath = hdfsHome+strippedDirectory+"/input";
		String hdfsOutputPath = hdfsHome+strippedDirectory+"/output"; 
		String hdfsTmpPath = hdfsHome+strippedDirectory+"/tmp"; 

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
			//fs.delete(new Path(hdfsHome+strippedDirectory), true);
			
			//String splitsPath = localInputPath+"/splitted";
			//InputSplitter splitter = new InputSplitter(localInputPath,splitsPath);
			//splitter.split();
			
			//copy splitted files from local Master to HDFS
			//fs.copyFromLocalFile(new Path(splitsPath), new Path(hdfsInputPath));
			
			fs.copyFromLocalFile(new Path(localInputPath), new Path(hdfsInputPath));
			
			//delete splitted files from local Master
			//fs.delete(new Path(splitsPath),true);

			Job job = Job.getInstance(conf, "HTS");
			job.setNumReduceTasks(0);
			job.setJarByClass(Main.class);

			job.setInputFormatClass(CommandFileInputFormat.class);
			// job.setOutputFormatClass(NullOutputFormat.class);

			job.setMapperClass(BaseMapper.class);
			job.setReducerClass(BaseReducer.class);

			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(NullWritable.class);

			FileInputFormat.setInputDirRecursive(job, true);
			FileInputFormat.addInputPath(job,new Path(hdfsInputPath));
			FileOutputFormat.setOutputPath(job, new Path(hdfsTmpPath));

			job.waitForCompletion(true);
		
			//Local directory will be created automatically if not exist
			//fs.copyToLocalFile(new Path(hdfsOutputPath),new Path(currentDirectory,localOutputPath));
			fs.copyToLocalFile(false,new Path(hdfsOutputPath),new Path(currentDirectory,localOutputPath),true);
			
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		finally
		{
			try 
			{
				//delete all job files on DataNode
			    for (String s: ClusterStats.getDatanodes())
			    {
			    	//Util.execute(String.format("ssh %s 'rm -fr job' ",s));
			    }
				//delete all job files on HDFS
				//fs.delete(new Path(hdfsHome+strippedDirectory), true);
				
			} 
			//catch (IllegalArgumentException | IOException e) 
			catch (Exception e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
