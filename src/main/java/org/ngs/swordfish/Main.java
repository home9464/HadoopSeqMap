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
	private static String statusUrl = "http://192.168.1.121:3000/job";
	
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
		//conf.set("mapreduce.job.end-notification.url","");
		
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
	
	private static void updateStatus(Job j)
	{
		//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
		if (statusUrl != null)
		{
			try {
				String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
					j.getJobID().toString(),
					j.getJobState().toString(),
					"Running job on cluster");
			
				Util.execute(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			}
		}
	}
	private static void updateStatus(String jobId,String jobState, String jobProgress,String jobMessage)
	{
		if (statusUrl != null)
		{
			//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
			String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobProgress\":\"%s\",\"jobMessage\":\"%s\"}",
					jobId,
					jobState,
					jobProgress,
					jobMessage);
			Util.execute(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
		}
	}

	private static void updateStatus(String jobId,String jobState, String jobMessage)
	{
		if (statusUrl != null)
		{
			//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
			String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
					jobId,
					jobState,
					jobMessage);
			Util.execute(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
		}
	}

	public static void main(String[] args)
	{
		
		Option i = OptionBuilder.withArgName( "input file or directory" ).hasArg().isRequired()
				.withDescription("Inputs for this job" ).create( "i" );

		Option o = OptionBuilder.withArgName( "output directory" ).hasArg().isRequired() 
				.withDescription("Output folder for this job" ).create( "o" );

		Option n = OptionBuilder.withArgName( "job name/id" ).hasArg().isRequired() 
				.withDescription("Job name" ).create( "n" );
		
		Options options = new Options();
		options.addOption(i);
		options.addOption(o);
		options.addOption(n);
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
		String jobId = null;
		try
		{
			line = parser.parse(options, args);
			localInputPath = line.getOptionValue("i");
			localOutputPath = line.getOptionValue("o");
			jobId = line.getOptionValue("n");
			
			fs = FileSystem.newInstance(conf);
			//fs.delete(new Path(hdfsHome+strippedDirectory), true);
			
			//copy splitted files from local Master to HDFS
			boolean delSrc = true;
			updateStatus(jobId,"RUNNING","Transfer input data to cluster");
			fs.copyFromLocalFile(delSrc,new Path(localInputPath), new Path(hdfsInputPath));
			
			//delete splitted files on local Master
			//fs.delete(new Path(localInputPath),true);

			Job job = Job.getInstance(conf, jobId);
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
			
			updateStatus(jobId,"RUNNING","Submit job to cluster");
			job.submit(); 
			
			//append hadoop app's id
			updateStatus(jobId+":"+job.getJobID().toString(),"RUNNING","Start hadoop job");
			while(!job.isComplete())
			{
				//updateStatus(job);
				//System.out.println("State:"+job.getJobState().toString());
				//System.out.println("Progress:"+String.valueOf(job.mapProgress()));
				Thread.sleep(5000);
			}
			//job.waitForCompletion(true);
			//Local directory will be created automatically if not exist
			//fs.copyToLocalFile(new Path(hdfsOutputPath),new Path(currentDirectory,localOutputPath));
			if(job.isSuccessful())
			{
				updateStatus(jobId,"RUNNING","Transfer output data to destination");
				delSrc = true;
				boolean useRawLocalFileSystem = true; //do not copy .crc files
				fs.copyToLocalFile(delSrc,new Path(hdfsOutputPath),new Path(currentDirectory,localOutputPath),useRawLocalFileSystem);
			}
			
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			if(jobId != null)
			{
				updateStatus(jobId,"FAILED",e.getMessage());
			}
			e.printStackTrace();
		}		
		finally
		{
			try 
			{
				updateStatus(jobId,"RUNNING","Clean up the cluster");
				//delete all job files on DataNode
			    for (String s: ClusterStats.getDatanodes())
			    {
			    	Util.execute(String.format("ssh %s 'rm -fr job' ",s));
			    }
				
				//delete all job files on HDFS
				fs.delete(new Path(hdfsHome+strippedDirectory), true);
				
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
