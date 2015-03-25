package org.ngs.swordfish;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

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
	//private final String COMMAND_FILE_EXTENSION=".cmd";
	private final String COMMAND_FILE="cmd.sh";
	
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
			if(fileName.equalsIgnoreCase(COMMAND_FILE))
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
			private boolean processed = false;
			private String commandFileName;
			private Path hdfsJobPath;
			private Configuration conf;

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
		    	/* hdfs://nn1:50017/user/hadoop/job/hadoop@scheduler/1/input/0002/1.cmd */
				
				FileSplit commandFileSplit = (FileSplit) arg0;
	    		conf = arg1.getConfiguration();
	    		
				commandFileName = commandFileSplit.getPath().getName();

	    		//base path to the command file in the HDFS. "hdfs://nn1:50017/user/hadoop/job/hadoop@scheduler/1/input/0002/"
				hdfsJobPath = commandFileSplit.getPath().getParent();
			}

			@Override
			public boolean nextKeyValue() throws IOException,InterruptedException
			{
				if (!processed)
				{
					StringBuffer sb = new StringBuffer();
					//The local path on DataNode
		    		String HDFS_scheme = "hdfs:/.+?:\\d+/";
		    		
		    		/* source: "hdfs://nn1:50017/user/hadoop/job/hadoop@scheduler/1/input/0002" 
		    		 * destionation: "/home/hadoop/job/hadoop@scheduler/1/input/0002_UUID"
		    		 * 
		    		 * 
		    		 * 
		    		 * source: "hdfs://nn1:50017/user/hadoop/hadoop_jobs/<job_id>/input/0002" 
		    		 * destionation: "/home/hadoop/job/hadoop@scheduler/1/input/0002_UUID"
		    		 * */
		    		String localJobPath = hdfsJobPath.toString().
		    				replaceAll(HDFS_scheme, "/").
							replaceFirst("/user/","/home/")+"_"+UUID.randomUUID().toString().replaceAll("-", "");
		    		
					FileSystem fs = FileSystem.get(conf);
					fs.copyToLocalFile(true,hdfsJobPath, new Path(localJobPath),true);
			        k = new Text(localJobPath+"/"+commandFileName);
					v = new Text("");
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
