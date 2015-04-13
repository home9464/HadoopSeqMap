package org.ngs.swordfish;
import java.io.IOException;
import java.util.UUID;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main
{
	//private static String statusUrl = "http://192.168.1.121:3000/job";
	private String jobId;
	private String hdfsBasePath;
	private String hdfsInputPath;
	private String hdfsOutputPath;
	private String hdfsTmpPath;
	
	private String localBasePath;
	private String statusUrl;
	
	private Configuration conf;
	private FileSystem fileSystem;
	
	public Main(CommandLine cmdLine)
	{
		conf = new Configuration();
		
		// "$HOME/A/B/C" -> "/A/B/C"
		/**
		currentDir = System.getProperty("user.dir");
		strippedDir = currentDir.replaceFirst(System.getProperty("user.home"),"");
		hdfsBasePath = String.format("/user/%s",System.getProperty("user.name"));
		hdfsInputPath = hdfsBasePath+strippedDir+"/input";
		hdfsOutputPath = hdfsBasePath+strippedDir+"/output";
		hdfsTmpPath = hdfsBasePath+strippedDir+"/tmp";
		*/
		
		//hdfsBasePath =  '/user/hadoop/jobs/1234'
		
		
		// '/user/hadoop/<hadoop_job_dirname>/22995/' ---> '/user/hadoop/<hadoop_job_dirname>/22995' 
		hdfsBasePath = FilenameUtils.normalizeNoEndSeparator(cmdLine.getOptionValue("d"),true);
		
		localBasePath = StringUtils.replaceOnce(hdfsBasePath,"/user","/home");
		
		//'/user/hadoop/<hadoop_job_dirname>/22995/input' 
		hdfsInputPath = hdfsBasePath+"/input";

		//'/user/hadoop/<hadoop_job_dirname>/22995/output' 
		hdfsOutputPath = hdfsBasePath+"/output";
		
		//'/user/hadoop/<hadoop_job_dirname>/22995/tmp' 
		hdfsTmpPath = hdfsBasePath+"/tmp";
		
		//jobId = 1234
		jobId = FilenameUtils.getName(hdfsBasePath);
		
		statusUrl = cmdLine.getOptionValue("u");
	}
	public void configureHadoop(int numContainersPerNode)
	{
		float ratio = 0.9f;

		long task_timeout_millsec = 259200000l; // 72 hours
		
		int memoryMbAvailable = (int) (ClusterStats.getInstance().getMemoryMbDN() * ratio);
		
		int cpuCores = ClusterStats.getInstance().getNumCpuCoresDN();
		
		//System.out.println("Memory:"+String.valueOf(memoryMbAvailable)+"MB,"+"CPU:"+ClusterStats.getNumCpuCoreDN());
		
		// int numContainersPerNode = memoryMbAvailable /
		// minMemoryMbPerContainer;

		// if (numContainersPerNode < 1)
		// {
		// throw new Exception(String.format(
		// "Insufficient memory: Asked: %d, Available: %d",
		// minMemoryMbPerContainer, memoryMbAvailable));
		// }
		int numCpuCoresPerContainer = ClusterStats.getInstance().getNumCpuCoresDN() / numContainersPerNode;
		numCpuCoresPerContainer = numCpuCoresPerContainer >= 1? numCpuCoresPerContainer:1;
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
	
	private void updateStatus(Job j)
	{
		//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
		String jsonContent="";
		try {
			jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
					j.getJobID().toString(),
					j.getJobState().toString(),
					"Running job on cluster");
		} catch (Exception e1) 
		{
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		if (statusUrl != null)
		{
			try {
				Util.command(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			} 
			catch (Exception e) 
			{
				System.err.println(e);
			} 
		}
		else
		{
			System.out.println(jsonContent);
		}
	}
	
	private void updateStatus(String jobState, String jobProgress,String jobMessage)
	{
		//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
		String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobProgress\":\"%s\",\"jobMessage\":\"%s\"}",
				jobId,
				jobState,
				jobProgress,
				jobMessage);
		if (statusUrl != null)
		{
			try
			{
				Util.command(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			}
			catch(Exception e)
			{
				System.err.println(e);

			}
			
		}
		else
		{
			System.out.println(jsonContent);
		}
	}

	private void updateStatus(String jobState, String jobMessage)
	{
		//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
		String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
				jobId,
				jobState,
				jobMessage);
		if (statusUrl != null)
		{
			try
			{
				Util.command(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			}
			catch(Exception e)
			{
				System.err.println(e);
			}
		}
		else
		{
			System.out.println(jsonContent);
		}
		
	}
	
	private void deleteLocalJobDir()
	{
		
		
		//'/user/hadoop/<hadoop_job_dirname>/22995' ---> 
		
		//delete all job files on DataNode
		for (final String s: ClusterStats.getInstance().getDatanodes())
		{
			updateStatus("RUNNING","Clean up local job temp files from "+s);
			//delete "job" folder on DataNode
			try 
			{
				new Thread(new Runnable() 
				{
				     public void run() 
				     {
							Util.command(String.format("ssh %s 'rm -fr %s'",s,localBasePath));
				     }
				}).start();
			}
			catch (Exception e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	public void start() throws Exception
	{
		try
		{
			
			fileSystem = FileSystem.newInstance(conf);
			configureHadoop(1);
			
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
			
			//the output path must NOT exist prior to launching job
			fileSystem.delete(new Path(this.hdfsTmpPath),true);
			
			FileInputFormat.addInputPath(job,new Path(this.hdfsInputPath));
			
			FileOutputFormat.setOutputPath(job, new Path(this.hdfsTmpPath));
			
			updateStatus("RUNNING","Submit job to cluster");
			job.submit(); 			
			//append hadoop app's id
			//mc.updateStatus(jobId+":"+job.getJobID().toString(),"RUNNING","Start hadoop job");
			updateStatus("RUNNING","Start hadoop job");
			
			while(!job.isComplete())
			{
				//updateStatus(job);
				Thread.sleep(5000);
			}
			
		}
		catch (Exception e) {
			updateStatus("Failed",e.getMessage());
			throw new Exception(e.getMessage());
		}		
		finally
		{
			deleteLocalJobDir();
		}
		
	}
	
	public static void main(String[] args) throws Exception
	{
		Option HdfsDir = OptionBuilder.hasArg().isRequired().create( "d" );
		Option PostUrl = OptionBuilder.hasArg().create( "u" );
		Options options = new Options();
		options.addOption(HdfsDir);
		options.addOption(PostUrl);
		CommandLineParser parser = new BasicParser();
		CommandLine  cmdLine = parser.parse(options, args);
		new Main(cmdLine).start();
	}
}
