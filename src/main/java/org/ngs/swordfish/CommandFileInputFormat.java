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
 * Process the command file
 * 
 * */
public class CommandFileInputFormat extends FileInputFormat<Text, Text>
{

	// private final String pathTmpLocal = String.format("/home/%s/tmp",user);
	private final String COMMAND_FILE_EXTENSION=".cmd";
	private final String pathHomeLocalDN = System.getProperty("user.home");
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
				commandFile = commandFileSplit.getPath();
				
				List<String> allFiles = new ArrayList<>() ;
				for (FileStatus f : listStatus(arg1))
				{
					allFiles.add(f.getPath().getName());
				}
				
				List<String> inputs= CommandFile.getFiles(commandFile,allFiles);

				inputFiles = new ArrayList<>();
				Path basePath = commandFile.getParent();
				for (String s:inputs)
				{
					inputFiles.add(new Path(basePath,s));
				}
				
	    		conf = arg1.getConfiguration();
				
	    		String jobPath = conf.get("JOB_WORKING_LOCAL_PATH");
	    		pathInputDN = pathHomeLocalDN + "/" + jobPath;
	    		
	    		//create working directory on DataNode's local disk
	    		new File(pathInputDN).mkdirs();
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
					
					//copy command file to DataNode
					String pathCmdFileDataNode = Util.copyFromHdfsToDNLocalFile(fs,commandFile,pathDst);
					//copy input files to DataNode
					for (Path p : inputFiles)
					{
						//"/PATH/A.bam /PATH/B.bam /PATH/C.bam" 
						sb.append(Util.copyFromHdfsToDNLocalFile(fs,p,pathDst) + " ");
					}
					
			        k = new Text(pathCmdFileDataNode);
					v = new Text(sb.toString().trim());
			        //k = new Text("HELLO");
					//v = new Text("WORLD");
					processed = true;
					return true;
				}
				return false;
			}

		};
	}

}
