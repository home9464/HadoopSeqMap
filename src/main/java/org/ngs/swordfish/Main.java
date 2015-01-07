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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main
{
	//private static String statusUrl = "http://192.168.1.121:3000/job";
	private String jobId;
	private String localInputPath = ".";
	private String localOutputPath = ".";
	private String statusUrl;
	static final Logger logger = LogManager.getLogger();

	private Configuration conf;
	private FileSystem fileSystem;
	public Main()
	{
		conf = new Configuration();
		
	}
	public Main(CommandLine cmdLine)
	{
		this();
		localInputPath = cmdLine.getOptionValue("i");
		localOutputPath = cmdLine.getOptionValue("o");
		jobId = cmdLine.getOptionValue("n");
		statusUrl = cmdLine.getOptionValue("u");
	}
	
	public Main(String jobid,String input,String output)
	{
		this();
		jobId = jobid;
		localInputPath = input;
		localOutputPath = output;
		statusUrl = null;
		conf = new Configuration();
		
	}
	public Main(String jobid,String input,String output,String statusurl)
	{
		this(jobid,input,output);
		statusUrl = statusurl;
	}
	
	public void configureHadoop(int numContainersPerNode)
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
	
	private void updateStatus(Job j)
	{
		//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
		if (statusUrl != null)
		{
			try {
				String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
					j.getJobID().toString(),
					j.getJobState().toString(),
					"Running job on cluster");
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			
			} catch (Exception e) 
			{
				// TODO Auto-generated catch block
				//e.printStackTrace();
			} 
		}
	}
	
	private void updateStatus(String jobState, String jobProgress,String jobMessage)
	{
		if (statusUrl != null)
		{
			//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
			String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobProgress\":\"%s\",\"jobMessage\":\"%s\"}",
					jobId,
					jobState,
					jobProgress,
					jobMessage);
			try
			{
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			}
			catch(Exception e)
			{
				
			}
			
		}
	}

	private void updateStatus(String jobState, String jobMessage)
	{
		if (statusUrl != null)
		{
			//{"JobId":"12334","JobStat":"RUNNING","JobProgress":0.75}
			String jsonContent = String.format("{\"jobId\":\"%s\",\"jobState\":\"%s\",\"jobMessage\":\"%s\"}",
					jobId,
					jobState,
					jobMessage);
			try
			{
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
			}
			catch(Exception e)
			{
				
			}
		}
	}
	
	private void transferInput(String hdfsDestPath) throws IllegalArgumentException, IOException
	{
		boolean delSrc = true;
		updateStatus("RUNNING","Transfer input data to cluster");
		fileSystem.copyFromLocalFile(delSrc,new Path(localInputPath), new Path(hdfsDestPath));
		
		//delete splitted files on local Master
		fileSystem.delete(new Path(localInputPath),true);
		
	}

	private void transferOutput(Path HDFS_OUTPUT_PATH, Path clientOutputPath)
	{
		updateStatus("RUNNING","Transfer output data to destination");
		boolean delSrc = true;
		boolean useRawLocalFileSystem = true; //do not copy .crc files
		//fileSystem.copyToLocalFile(delSrc,new Path(HDFS_OUTPUT_PATH),new Path(CURRENT_DIR,localOutputPath),useRawLocalFileSystem);
		try 
		{
			fileSystem.copyToLocalFile(delSrc,HDFS_OUTPUT_PATH,clientOutputPath,useRawLocalFileSystem);
		} 
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
	}

	private void cleanup(Path hdfsPathToClean)
	{
		try 
		{
			updateStatus("RUNNING","Clean up the temporary HDFS files");
			//delete all job files on DataNode
		    //for (String s: ClusterStats.getDatanodes())
		    //{
		    	//Util.execute(String.format("ssh %s 'rm -fr job' ",s));
		    //}
			
			//delete job folder
		    fileSystem.delete(hdfsPathToClean, true);
			
		} 
		//catch (IllegalArgumentException | IOException e) 
		catch (Exception e) 
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void start()
	{
		String CURRENT_DIR = System.getProperty("user.dir");
		// "$HOME/A/B/C" -> "/A/B/C" 
		String STRIPPED_DIR = CURRENT_DIR.replaceFirst(System.getProperty("user.home"),"");
		String HDFS_HOME = String.format("/user/%s",System.getProperty("user.name"));
		String HDFS_INPUT_PATH = HDFS_HOME+STRIPPED_DIR+"/input";
		String HDFS_OUTPUT_PATH = HDFS_HOME+STRIPPED_DIR+"/output";
		String HDFS_TMP_PATH = HDFS_HOME+STRIPPED_DIR+"/tmp";
		try
		{
			
			fileSystem = FileSystem.newInstance(conf);
			configureHadoop(1);
			
			//copy splitted files from local Master to HDFS
			transferInput(HDFS_INPUT_PATH);
			
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
			FileInputFormat.addInputPath(job,new Path(HDFS_INPUT_PATH));
			FileOutputFormat.setOutputPath(job, new Path(HDFS_TMP_PATH));
			
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
			if(job.isSuccessful())
			{
				transferOutput(new Path(HDFS_OUTPUT_PATH), new Path(CURRENT_DIR,localOutputPath));
			}
			
		}
		catch (Exception e) {
			updateStatus("FAILED",e.getMessage());
		}		
		finally
		{
			//cleanup(new Path(HDFS_HOME+STRIPPED_DIR));
		}
		
	}
	
	public static void main(String[] args)
	{
		Option optInput = OptionBuilder.hasArg().isRequired().create( "i" );
		Option optOuptut = OptionBuilder.hasArg().isRequired() .create( "o" );
		Option optJobId = OptionBuilder.hasArg().isRequired().create( "n" );
		Option optPostUrl = OptionBuilder.hasArg().create( "u" );
		
		Options options = new Options();
		options.addOption(optInput);
		options.addOption(optOuptut);
		options.addOption(optJobId);
		options.addOption(optPostUrl);
		CommandLineParser parser = new BasicParser();
		try 
		{
			CommandLine cmdLine = parser.parse(options, args);
			new Main(cmdLine).start();
		} 
		catch (ParseException e) 
		{
			e.printStackTrace();
		}
		
	}
}
