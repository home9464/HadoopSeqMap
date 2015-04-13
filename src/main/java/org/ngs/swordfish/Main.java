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
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
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
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
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
				Util.runCommand(String.format("curl -d '%s' -H \"Content-Type: application/json\" %s",jsonContent,statusUrl));
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
		for (final String s: ClusterStats.getInstance().getDataNodes())
		{
			updateStatus("RUNNING","Clean up local job temp files from "+s);
			//delete "job" folder on DataNode
			try 
			{
				new Thread(new Runnable() 
				{
				     public void run() 
				     {
							try
							{
								Util.runCommand(String.format("ssh %s 'rm -fr %s'",s,localBasePath));
							}
							catch (Exception e)
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
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
