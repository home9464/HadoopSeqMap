package org.ngs.swordfish;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BaseMapper extends Mapper<Text, Text, NullWritable, NullWritable>
{
	//public final String user = System.getProperty("user.name");
	// the temporary path on HDFS
	//public final String pathOutputHdfs = String.format("/user/%s/job/output/", user);
	//public final String CpuNum = Integer.toString(Runtime.getRuntime().availableProcessors());
	/**
	 * The key is the full path of command file on DataNode. "/home/hadoop/job/1/A.cmd"
	 * 
	 * ~/job/hadoop@scheduler/1/input/1:
	 * */
	public void map(Text key, Text value, Context context)
	{
		//"Hadoop speculative task execution" will cause a problem here.
		//mapreduce.map.speculative=false
		//mapreduce.reduce.speculative=false
		
		
		/* commandFile: /home/hadoop/job/hadoop@scheduler/1/input/0002/1.cmd */
		
		
		String commandFile = key.toString();

		
		/* workingPath: /home/hadoop/job/hadoop@scheduler/1/input/0002 */
		String workingPath  = FilenameUtils.getFullPathNoEndSeparator(commandFile);
		
		//strippedPath: job/hadoop@scheduler/1/input/0002
		String strippedPath = workingPath.replaceFirst(System.getProperty("user.home"),"");

		/* /user/hadoop/job/hadoop@scheduler/1/output/0002 */
		String outputPath = String.format("/user/%s/%s", System.getProperty("user.name"), strippedPath).replace("/input/", "/output/");
		
		Path hdfsOutputPath = new Path(outputPath);
		
		/* 1.cmd */
		String commandFileName  = FilenameUtils.getName(commandFile);

		boolean successed = false;
		
		final Configuration conf = context.getConfiguration();
		
		FileSystem fs = null;

		// Runtime.getRuntime().addShutdownHook(new Thread() {
		//	@Override
        //   public void run() {
		//		Util.putStatusDebug(conf.get("statusUrl"), "Error","Inside Add Shutdown Hook");
        //   }   
        //}); 
		
		try
		{

			fs = FileSystem.newInstance(conf);

			//use timestamp to determine output files  - newer files are outputs
			long  timeStamp = 0L;
			File[] allFiles = new File(workingPath).listFiles();		
			for(File f:allFiles)
			{
				if(f.lastModified() > timeStamp)
				{
					timeStamp = f.lastModified();
				}
			}
			
			//run the command file as a script
			
			//should enable log aggregation
			
			//default location for "stderr" 
			//hadoop-2.6.0/logs/userlogs/application_1425921303294_0026/container_1425921303294_0026_01_000002/stderr
			
			//default location for "stdout" 
			//hadoop-2.6.0/logs/userlogs/application_1425921303294_0026/container_1425921303294_0026_01_000002/stdout

			//run the script
			int retVal = Util.runScript(commandFile);
			//System.out.println("#RET#"+String.valueOf(retVal));
			
		
			//list all files under working directory, again
			allFiles = new File(workingPath).listFiles();
			List<String> outputFiles = new ArrayList<>();
			for(File f:allFiles)
			{
				if(f.lastModified() > timeStamp && f.length()>0)
				{
					outputFiles.add(f.getAbsolutePath());
				}
			}
			
			fs.mkdirs(hdfsOutputPath);
			
			for(String output: outputFiles)
			{
				//transfer outputs from DataNode to HDFS
				Util.putStatusDebug(conf.get("statusUrl"), 
						"Running","Transfer output from "+output+" to "+hdfsOutputPath.toString());
				fs.copyFromLocalFile(new Path(output), hdfsOutputPath);
			}
			successed = true;
			
		}
		catch (java.nio.channels.ClosedChannelException e)
		{
			Util.putStatusDebug(conf.get("statusUrl"),"Running",e.toString());
			
		}
		catch (org.apache.commons.exec.ExecuteException e)
		{
			Util.putStatusDebug(conf.get("statusUrl"),"Running",e.toString());
			
		}
		catch (Exception e)
		{
			Util.putStatusError(conf.get("statusUrl"),"Error",e.toString());
		}
		finally
		{
			//if a job failed (or killed), the partial outputs should not be transfered back
			if (!successed && fs!=null)
			{
				boolean recursive = true; 
				try
				{
					fs.delete(hdfsOutputPath, recursive);
				}
				catch (Exception e)
				{
					Util.putStatusDebug(conf.get("statusUrl"), "Error",e.toString());
				}
			}
		}
	}
}
