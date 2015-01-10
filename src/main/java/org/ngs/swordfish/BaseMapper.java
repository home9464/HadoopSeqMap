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
		/* commandFile: /home/hadoop/job/hadoop@scheduler/1/input/0002/1.cmd */
		String commandFile = key.toString();

		
		/* workingPath: /home/hadoop/job/hadoop@scheduler/1/input/0002 */
		String workingPath  = FilenameUtils.getFullPathNoEndSeparator(commandFile);
		
		//strippedPath: job/hadoop@scheduler/1/input/0002
		String strippedPath = workingPath.replaceFirst(System.getProperty("user.home"),"");

		/* 1.cmd */
		String commandFileName  = FilenameUtils.getName(commandFile);

		
		try
		{

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
			Configuration conf = context.getConfiguration();
			
			Util.runScript(workingPath,commandFileName);
			
			FileSystem fs = FileSystem.newInstance(conf);
		
			//list all files under working directory, again
			allFiles = new File(workingPath).listFiles();		
			List<String> outputFiles = new ArrayList<>();
			for(File f:allFiles)
			{
				if(f.lastModified() > timeStamp)
				{
					outputFiles.add(f.getAbsolutePath());
				}
			}
			
			/* /user/hadoop/job/hadoop@scheduler/1/output/0002 */
			String outputPath = String.format("/user/%s/%s", 
					System.getProperty("user.name"),
					strippedPath).replace("/input/", "/output/");
			
			Path hdfsOutputPath = new Path(outputPath);
			
			fs.mkdirs(hdfsOutputPath);
			
			for(String output: outputFiles)
			{
				//copy results from DataNode's local disk to HDFS
				fs.copyFromLocalFile(new Path(output), hdfsOutputPath);
			}
			
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			
			try {
				//Util.runCommand(String.format("rm -fr %s",workingPath));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}
}
