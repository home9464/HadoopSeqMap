package org.ngs.swordfish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * Process (split) command file ends wth ".cmd"
 * 
 * */
public class CommandFileInputFormat extends FileInputFormat<Text, Text>
{

	// private final String pathTmpLocal = String.format("/home/%s/tmp",user);
	private final String COMMAND_FILE_EXTENSION=".cmd";
	//private final String pathHomeLocalDN = System.getProperty("user.home");
	private String pathInputDN;
	
	@Override
	protected boolean isSplitable(JobContext context, Path file)
	{
		return false;
	}

	/**
	 * get the list of command files "*.cmd"
	 * 
	 * */
	@Override
	public List getSplits(JobContext job) throws IOException
	{

		List<FileSplit> splits = new ArrayList<>();
		List<FileStatus> files = listStatus(job);
		for (FileStatus file : files)
		{
			String fileName = file.getPath().getName();
			if(fileName.endsWith(COMMAND_FILE_EXTENSION))
			//if(FilenameUtils.getExtension(fileName).equalsIgnoreCase(COMMAND_FILE_EXTENSION))
			{
		    	 //hdfs://nn1:50017/user/hadoop/input/A_0000_R1.fq<PATH_SEPARATOR>hdfs://nn1:50017/user/hadoop/input/A_0000_R2.fq<PATH_SEPARATOR>A_0000
				splits.add(new FileSplit(file.getPath(), 0, 0, null));
			}
		}
		return splits;
	}

	@Override
	public RecordReader<Text, Text> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException
	{

		return new RecordReader<Text, Text>()
		{

			private Text k;
			private Text v;
			private Path commandFile;
			private List<Path> inputFiles;
			private String jobFolderName;
			private boolean processed = false;
			Configuration conf;
			//private FileSystem fs;

			@Override
			public void close() throws IOException
			{}

			@Override
			public Text getCurrentKey() throws IOException,InterruptedException
			{
				return k;
			}

			@Override
			public Text getCurrentValue() throws IOException,InterruptedException
			{
				// the full local path of the input file
				return v;
			}

			@Override
			public float getProgress() throws IOException, InterruptedException
			{
				return 0;
			}

			@Override
			public void initialize(InputSplit arg0, TaskAttemptContext arg1)
					throws IOException, InterruptedException
			{
				FileSplit commandFileSplit = (FileSplit) arg0;
		    	//hdfs://nn1:50017/user/hadoop/job/A/B/C/1.cmd
				// /user/hadoop/job/hadoop@scheduler/1/input/A_0000.cmd
				commandFile = commandFileSplit.getPath();
				
				
				// "1.cmd" -> "1"
				jobFolderName = FilenameUtils.getBaseName(commandFile.getName());
				
				List<String> allFiles = new ArrayList<>() ;
				for (FileStatus f : listStatus(arg1))
				{
					allFiles.add(f.getPath().getName());
				}
				
				//get the list of files that appear in the command from total files. 
				List<String> inputs= CommandFile.getFiles(commandFile,allFiles);

				inputFiles = new ArrayList<>();
				// "/user/hadoop/job/hadoop@scheduler/1/input/"
				Path basePath = commandFile.getParent();
				for (String s:inputs)
				{
					inputFiles.add(new Path(basePath,s));
				}
				
	    		conf = arg1.getConfiguration();
				
	    		String HDFS_scheme = "hdfs:/.+?:\\d+/";
	    		
	    		
	    		// "hdfs://nn1:50017/user/hadoop/job/A/B/C/1.cmd" -> "/user/hadoop/job/A/B/C/1.cmd" -> "/home/hadoop/job/A/B/C/1.cmd" 
	    		//String hdfsHome = String.format("/user/%s/",System.getProperty("user.name"));
	    		//String localHome = System.getProperty("user.home");
	    		//String strippedPath = commandFile.toString().replaceAll(HDFS_scheme, "/").replaceFirst(hdfsHome,"/");
	    		//pathInputDN = localHome + "/" + strippedPath;
	    		
	    		 // HDFS:/user/hadoop/job/A/B/C/input/A_0000_1.cmd -> DataNode:/home/hadoop/job/A/B/C/A_0000_1/
	    		pathInputDN = basePath.getParent().toString().
	    										replaceAll(HDFS_scheme, "/").
	    										replaceFirst("/user/","/home/")+"/"+jobFolderName+"/";
	    		
	    		
	    		//if exist
	    		//new File(pathInputDN).delete();
	    		
	    		//create working directory on DataNode's local disk
	    		//new File(pathInputDN).mkdirs();
	    		
	    		
			}

			@Override
			public boolean nextKeyValue() throws IOException,InterruptedException
			{
				if (!processed)
				{
					StringBuffer sb = new StringBuffer();
					//The local path on DataNode
					Path pathDst = new Path(pathInputDN);
					
					FileSystem fs = FileSystem.get(conf);
					
					//copy command file from HDFS to DataNode
					String pathCmdFileDataNode = copyFromHdfsToDNLocalFile(fs,commandFile,pathDst);
					
					//copy input files from HDFS to DataNode
					for (Path p : inputFiles)
					{
						//"/PATH/A.bam /PATH/B.bam /PATH/C.bam"
						sb.append(copyFromHdfsToDNLocalFile(fs,p,pathDst) + " ");
					}
					
			        k = new Text(pathCmdFileDataNode);
					v = new Text(sb.toString().trim());
					processed = true;
					return true;
				}
				return false;
			}
			
			/**
			 * Copy file from HDFS to local directory on DataNode 
			 * */
			private String copyFromHdfsToDNLocalFile(FileSystem fs,Path pathSrc,Path pathDst)throws IOException
			{
				File fDir = new File(pathDst.toUri().toString());
				if(!fDir.exists())
				{
					fDir.mkdirs();
				}
				String HDFS_scheme = "hdfs:/.+?:\\d+/";
				Path pathNormalized = new Path(pathSrc.toString().replaceAll(HDFS_scheme, "/"));
				fs.copyToLocalFile(false,pathNormalized, pathDst,true);
				return pathDst+"/"+pathSrc.getName();
			}
		};
	}

}
