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

public class BaseMapper extends Mapper<Text, Text, NullWritable, NullWritable>
{
	//public final String user = System.getProperty("user.name");
	// the temporary path on HDFS
	//public final String pathOutputHdfs = String.format("/user/%s/job/output/", user);
	//public final String CpuNum = Integer.toString(Runtime.getRuntime().availableProcessors());
	/**
	 * The key is the full path of command file on DataNode. "/home/hadoop/job/1/A.cmd"
	 * 
	 * */
	public void map(Text key, Text value, Context context)
	{
		
		String commandFile = key.toString();

		//the local path of command file on DataNode -> "/home/<user>/A/B/C" 
		String workingPath  = FilenameUtils.getFullPathNoEndSeparator(commandFile);
		
		// "/home/user/A/B/C" -> "A/B/C"
		String strippedPath = workingPath.replaceFirst(System.getProperty("user.home"),"");
		
		try
		{
			String cmdFileName  = FilenameUtils.getName(commandFile);

			//run the command file as a script
			Configuration conf = context.getConfiguration();
			String ret = Util.executeShellStdout(workingPath,cmdFileName);
			if (ret.startsWith("Exception") || ret.startsWith("Error")) 
			{
				throw new IOException(ret);
			}
			
			FileSystem fs = FileSystem.newInstance(conf);
		
			//FIXME: collect output files only.
			// copy output file from DataNode's local disk to HDFS
		
			//list all files under working directory
			File[] ffiles = new File(workingPath).listFiles();		
			List<String> allFiles = new ArrayList<>();
			List<String> cmdFiles = new ArrayList<>();
			for(File f:ffiles)
			{
				if(f.getName().endsWith(".cmd"))
				{
					cmdFiles.add(f.getAbsolutePath());
				}
				else if(f.getName().equalsIgnoreCase("stderr.txt"))
				{
					cmdFiles.add(f.getAbsolutePath());
				}
				else
				{
					allFiles.add(f.getAbsolutePath());
				}
			}
			//list all known input files
			List<String> inputFiles = Arrays.asList(value.toString().split(" "));
		
			//exclude input files from all files, then remaining files are outputs
			allFiles.remove(commandFile);
			allFiles.removeAll(inputFiles);
			
			/*
			List<String> otherInputs = new ArrayList<>();
			for (String c:cmdFiles)
			{
				otherInputs.addAll(CommandFile.getFiles(new Path(c),allFiles));
			}
			allFiles.removeAll(otherInputs);
			*/
			
			
			//copy remaining files to designated HDFS path for this job
		
			// "/home/hadoop/job/hadoop@scheduler/1" -> "/job/hadoop@scheduler/1"
			
		
			//output dir on HDFS, "/user/<username>/A/B/C/output"
			//"/user/hadoop/job/hadoop@scheduler/1/input/A_100k_0003_R2.fastq.gz"

			// strippedPath = "job/hadoop@scheduler/1/A_0003"
			// outputPath = "/user/hadoop/job/hadoop@scheduler/1/output/"

			//Path pathOutputHdfs = new Path(String.format("/user/%s/%s/output/", System.getProperty("user.name"),strippedPath));
			Path pathOutputHdfs = new Path(String.format("/user/%s/%s/output/", System.getProperty("user.name"),FilenameUtils.getPath(strippedPath)));
		 
			fs.mkdirs(pathOutputHdfs);
			for(String output: allFiles)
			{
				//copy results from DataNode's local disk to HDFS
				fs.copyFromLocalFile(new Path(output), pathOutputHdfs);
			}
			
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			//Util.execute(String.format("rm -fr %s",workingPath));
		}

	}
}
