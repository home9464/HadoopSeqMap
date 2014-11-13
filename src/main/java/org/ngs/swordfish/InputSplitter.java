package org.ngs.swordfish;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UnknownFormatFlagsException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;

public class InputSplitter 
{
	private int numContainersPerNode;
	private final int FASTQ_NUM_LINES_PER_READ = 4;
	private String inputPath;
	private String outputPath;
	private final String APP_PATH=System.getProperty("user.home")+"/"+"bio/app";
	
	private final String fastqPattern = "(.*?)[\\._][R]?[1|2](.*?).f(ast)?q(.gz)?";
	private final String patternR1 = "(.*?)[\\._][R]?[1](.*?).f(ast)?q(.gz)?";
	private final String patternR2 = "(.*?)[\\._][R]?[2](.*?).f(ast)?q(.gz)?";
	private final String commandFileName = "cmd.txt";
	
	
	private final String separator = "_";
	

	public InputSplitter(String localInputPath,String localOutputPath)
	{
		numContainersPerNode = 1;
		inputPath = localInputPath;
		outputPath = localOutputPath;
	}
	
	public void split() throws Exception
	{
		Map<String,String> pairs = pairFastq(listFiles(inputPath,"*.{fastq,fq,fastq.gz,fq.gz}"));
		for (Map.Entry<String, String> entry : pairs.entrySet())
		{
			String[] fastqs = entry.getValue().split(",");
			String[] splits = splitFastq(fastqs[0],fastqs[1],10);
			Map<String,String> splitPairs = pairFastq(Arrays.asList(splits));
			for (Map.Entry<String, String> splitentry : splitPairs.entrySet())
			{
				String sampleName = splitentry.getKey();
				String[] splitfastqs = splitentry.getValue().split(",");
				String splitfastqsR1 = splitfastqs[0]; 
				String splitfastqsR2 = splitfastqs[1];
				
				System.out.println(sampleName);
				
			}
			
		}
		List<String> bamFiles = listFiles(inputPath,"*.{bam}");
		for (String bam : bamFiles)
		{
			splitBam(bam);
		}
		
	}
	public void split2() throws Exception
	{
		int fragments = 10; 
		//System.out.println(String.format("rm -fr %s/%s",inputPath,SPLIT_FOLDER_NAME));
		Util.execute(String.format("rm -fr %s",outputPath));
		
		if(listFiles(inputPath,"*.{fastq,fq,fastq.gz,fq.gz}").size()>0)
		{
			System.out.println(String.format("%s/util/utool fastq %s %s %d",APP_PATH, inputPath,outputPath,fragments));
			Util.execute(String.format("%s/util/utool fastq %s %s %d",APP_PATH, inputPath,outputPath,fragments));
		}
		else
		{
			Util.execute(String.format("%s/util/utool bam %s %s",APP_PATH, inputPath,outputPath));
		}
	}
	
    private List<String> listFiles(String directory, String pattern)
    {
    	List<String> files = new ArrayList<>();
        DirectoryStream<Path> directoryStream = null;
        try{
            directoryStream = Files.newDirectoryStream(Paths.get(directory), pattern);
            for(Path path : directoryStream)
            {
            	files.add(path.toString());
                //System.out.println("Files/Directories matching "+ pattern +": "+ path.toString());
            }
        }catch(IOException ioe)
        {
            ioe.printStackTrace();
        }
        finally
        {
            try
            {
                directoryStream.close();
            }
            catch(IOException ioe)
            {
                //Do Nothing
            }
        }
        return files;
    }	
    
	private Map<String,String> pairFastq(List<String> fastqs) throws UnknownFormatFlagsException
	{
		Pattern pattern = Pattern.compile(fastqPattern);
		Map<String,String> rets = new HashMap<>();
		String sampleName = null;
		
		for (String qs: fastqs)
		{
			
			String fastqBaseName = FilenameUtils.getBaseName(qs);
	        Matcher matcher = pattern.matcher(fastqBaseName);
			if(matcher.find())
			{
	        	sampleName = matcher.group(1)+matcher.group(2);
			}
			else
			{
				String sample = ""; 
				String lane = ""; 
				String remain = ""; 
				
				String[] parts = fastqBaseName.split(separator);
				if(parts.length ==3)
				{
					sample = parts[0]; 
					lane = parts[1]; 
					remain = parts[2]; 
				}
				else if(parts.length ==2)
				{
					sample = parts[0]; 
					remain = parts[1]; 
				}
				else
				{
					//System.out.println("illegal format");
					throw new UnknownFormatFlagsException(fastqBaseName);
				}
				sampleName = sample+lane;
			}
				
	        
			
			if(rets.containsKey(sampleName))
			{
				//make sure in the correct order as "A_R1.fq,A_R2.fq", not "A_R2.fq,A_R1.fq"  
				if(rets.get(sampleName).matches(patternR2))  //it is the R2
				{
					rets.put(sampleName, qs+","+rets.get(sampleName));
				}
				else
				{
					rets.put(sampleName, rets.get(sampleName)+","+qs);
				}
			}
			else
			{
				rets.put(sampleName,qs);
			}
		}
		return rets;
		
	}
    
	private String[] splitFastq(String fastq_1,String fastq_2,int numFragments) throws Exception
	{

		/*
		 * <prefix>_R1_<suffix>.fastq.gz
		 * 
		 * 
		 */
        Matcher matcher = Pattern.compile(patternR1).matcher(FilenameUtils.getBaseName(fastq_1));
		String sampleName = null;
        while (matcher.find()) 
        {
        	sampleName = matcher.group(1)+matcher.group(2);
        }
        if(sampleName==null)
        {
        	sampleName = FilenameUtils.getBaseName(Util.getCommonPrefix(fastq_1,fastq_2));
        }
		
		String cat = fastq_1.endsWith(".gz") ? "pigz -dc" : "cat";

		// we have to count how many lines per FASTQ file.
		String ret = Util.executeStdout(String.format("%s %s | wc -l", cat,fastq_1));
		
		int totalReads = Integer.parseInt(ret)/FASTQ_NUM_LINES_PER_READ;

		//System.out.println(String.format("Total reads: %d ", totalReads));
		
		if (ret == null || ret == "0")
		{
			throw new Exception("Empty fastq file");
		}

		//default. use 0.9*TOTAL memory
		
		/*
		float ratioMemoryForContainer = 0.9f;
		
		int mbMemAvailable = (int) (ClusterStats.getMemoryMbDN() * ratioMemoryForContainer);
		
		System.out.println(String.format("Available memory: %d ", mbMemAvailable));
		
		default. split raw fastq into 2*TOTAL_CONTAINERS fragments
		
		int numFragments = numContainersPerNode * ClusterStats.getNumDN()*2; 
		
		int numReadsPerFragment = (int) (Math.ceil((totalReads*1.0)/ numFragments ));

		while (numReadsPerFragment / mbMemAvailable > 200) //shrink fragment if too large
		{
			//numFragments = numFragments + 1;
			numFragments = numFragments * 2;
			numReadsPerFragment = (int) (Math.ceil((totalReads*1.0)/ numFragments ));
		}

		*/
		//int numFragments = numContainersPerNode * 4;
		
		int numReadsPerFragment = (int) (Math.ceil((totalReads*1.0)/ numFragments ));
		
		//System.out.println(String.format("FASTQ file splits: %d ", numFragments));

		// create output path
		// System.out.println("Create output path: "+pathOfOutput);
		Util.execute(String.format("mkdir -p %s", outputPath));

		class Splitter implements Runnable
		{

			private String filename;
			private String splitcat;
			private int readIndex;
			private int lpf; // linesPerFragment
			String pathout;
			String samplename;

			public Splitter(String fastqFileToBeSplitted, int readindex,String cmdCat,
					int linesPerFragment, String pathOfOutput, String sampleName)
			{
				filename = fastqFileToBeSplitted;
				readIndex = readindex;
				splitcat = cmdCat; //either "cat" or "zcat"
				samplename = sampleName;
				lpf = linesPerFragment;
				pathout = pathOfOutput;
			}

			public void run()
			{
				final String prefix = samplename + String.format("_%d_", readIndex);
				String cmd = String.format(
						"%s %s | split -l %d -a 4 -d - %s/%s", splitcat,
						filename, lpf, pathout, prefix);
				Util.execute(cmd);
				String[] listOfSplittedFiles = new File(pathout)
						.list(new FilenameFilter()
						{
							public boolean accept(File directory,
									String fileName)
							{
								return fileName.startsWith(prefix);
							}
						});

				for (String s : listOfSplittedFiles)
				{
					String suffix = s.replaceAll(prefix, "");
					String targetFileName = String.format("%s_%s_R%d.fastq",
							samplename, suffix, readIndex);
					// System.out.println(String.format("mv %s/%s %s/%s",pathout,s,pathout,targetFileName));
					Util.execute(String.format("mv %s/%s %s/%s", pathout,
							s, pathout, targetFileName));
				}
			}
		}
		// start a thread pool to accelerate the splitting of the two raw FASTQ
		// files
		ExecutorService pool = Executors.newFixedThreadPool(2);
		pool.execute(new Splitter(fastq_1, 1,cat, numReadsPerFragment*FASTQ_NUM_LINES_PER_READ, outputPath, sampleName));
		pool.execute(new Splitter(fastq_2, 2,cat, numReadsPerFragment*FASTQ_NUM_LINES_PER_READ, outputPath, sampleName));
		pool.shutdown();
		pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

		// start a thread pool to accelerate the gzipping of splitted FASTQ
		// files
		boolean gzipOutput = true;
		
		if (gzipOutput)
		{
			class Gzipper implements Runnable
			{

				private String fileName;

				public Gzipper(String filename)
				{
					fileName = filename;
				}

				public void run()
				{
					Util.execute(String.format("pigz %s", fileName));
				}
			}

			// Runtime.getRuntime().availableProcessors()
			ExecutorService pool2 = Executors.newFixedThreadPool(2);
			for (File f : new File(outputPath).listFiles())
			{
				pool2.execute(new Gzipper(f.getAbsolutePath()));
			}
			pool2.shutdown();
			pool2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		}

		return new File(outputPath).list(
				new FilenameFilter()
				{
					public boolean accept(File directory,String fileName)
					{
						return fileName.endsWith("fastq.gz");
					}
				});
		
		
	}

	private void splitBam(String bamFile)
	{
		
	}

}
