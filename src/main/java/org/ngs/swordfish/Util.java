package org.ngs.swordfish;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.UnknownFormatFlagsException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class Util
{

	private final static String separator = "_";

	public static Map<String,String> pairFastq(List<String> fastqs) throws UnknownFormatFlagsException
	{
		Map<String,String> rets = new HashMap<>();
		for (String qs: fastqs)
		{
			
			String s = FilenameUtils.getName(qs);

			String sample = ""; 
			String lane = ""; 
			String remain = ""; 
			
			String[] parts = s.split(separator);
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
				throw new UnknownFormatFlagsException(s);
			}
			
			String k = sample+lane;
			
			if(rets.containsKey(k))
			{
				//make sure in the correct order as "A_R1.fq,A_R2.fq", not "A_R2.fq,A_R1.fq"  
				if(rets.get(k).matches(".*_R?2.f(ast)?q(.gz)?"))  //it is the R2
				{
					rets.put(k, qs+","+rets.get(k));
				}
				else
				{
					rets.put(k, rets.get(k)+","+qs);
				}
			}
			else
			{
				rets.put(k,qs);
			}
		}
		return rets;
		
	}

	public static String getCommonPrefix(List<String> strings)
	{
		String[] sz = new String[strings.size()];
		strings.toArray(sz);
		return getCommonPrefix(sz);
	}

	public static String getCommonPrefix(String[] strings)
	{
		String prefix = new String();
        if(strings.length > 0)
            prefix = strings[0];
        for(int i = 1; i < strings.length; ++i) {
            String s = strings[i];
            int j = 0;
            for(; j < Math.min(prefix.length(), s.length()); ++j) {
                if(prefix.charAt(j) != s.charAt(j)) {
                    break;
                }
            }
            prefix = prefix.substring(0, j);
        }
        //return prefix.equals("")?UUID.randomUUID().toString():prefix;
        //return prefix.equals("")?String.format("sample_%s", Long.toString(System.currentTimeMillis())+"_"+RandomStringUtils.randomAlphabetic(8).toUpperCase()):prefix;
        //prefix=prefix.equals("")?"sample":prefix;
        return prefix.equals("")? "sample-"+String.format("%s", Long.toString(System.currentTimeMillis())+"-"+RandomStringUtils.randomAlphabetic(8).toUpperCase()):prefix;
	}

	public static String getCommonPrefix(String path) throws IOException
	{
		List<String> fs = new ArrayList<>();
		DirectoryStream<java.nio.file.Path> stream = Files.newDirectoryStream(Paths.get(path)); 
		for (java.nio.file.Path entry: stream) 
		{
			fs.add(entry.getFileName().toString());
		}
		String[] zfs = new String[fs.size()];
		fs.toArray(zfs);
		return getCommonPrefix(zfs);
	}

	
	public static String copyFromHdfsToDNLocalFile(FileSystem fs,Path pathSrc,Path pathDst)
			throws IOException
	{
		File fDir = new File(pathDst.toUri().toString());
		if(!fDir.exists())
		{
			fDir.mkdirs();
		}
		String HDFS_scheme = "hdfs:/.+?:\\d+/";
		Path pathNormalized = new Path(pathSrc.toString().replaceAll(HDFS_scheme, "/"));
		//File temp_data_file = File.createTempFile(pathSrc.getName(), "",new File(pathDst.getName()));
		// File temp_data_file =
		// File.createTempFile(some_path.getName(),"");
		//temp_data_file.deleteOnExit();
		fs.copyToLocalFile(false,pathNormalized, pathDst,true);
		
		return pathDst+"/"+pathSrc.getName();
	}

	public static void searchExecutableScript(String[] appPaths)
	{
		/*
		for (String p: appPaths)
		{
			String findScript = String.format("file -i `find %s -name \"*.%s\" 2>/dev/null`",p,"sh");
			System.out.println(findScript);

			String[] scripts = Util.execShellCommand(findScript);
			for (String s: scripts)
			{
				System.out.println(s);
			}
			
		}*/
	}
	
	/**
	 * TODO: return (exitcode, stdout, stderr)
	 * 
	 * */
    public static int execute(final String command)
    {
    	try
    	{
  		  	String [] cmd = {"/bin/bash" , "-c", command};
  		  	Process p = Runtime.getRuntime().exec(cmd);
  		  	BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));  
  		  	String line = null;  
  		  	while ((line = in.readLine()) != null) 
  		  	{  
  		  		//System.out.println(line);  
  		  	} 
  		  	return p.waitFor();

    	}
    	catch(IOException e)
    	{
			e.printStackTrace();
    		
    	}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		return -1;
    }

    /**
     * run a shell script
     * 
     * */
    public static int executeScript(final String script)
    {
    	try
    	{
  		  	String [] cmd = {"/bin/bash" ,script};
  		  	ProcessBuilder probuilder = new ProcessBuilder(cmd);
  		  	Map<String, String> env = probuilder.environment();
			env = System.getenv();
  		  	Process process = probuilder.start();
  		  	BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));  
  		  	String line = null;  
  		  	while ((line = in.readLine()) != null) 
  		  	{  
  		  		//System.out.println(line);  
  		  	} 
  		  	return process.waitFor();

    	}
    	catch(IOException e)
    	{
			e.printStackTrace();
    		
    	}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		return -1;
    }

    public static String executeStdout(String command)
    {
    	try
    	{
  		  	String [] cmd = {"/bin/bash" , "-c", command};
  		  	Process p = Runtime.getRuntime().exec(cmd);
  		  	p.waitFor();
  		  	BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
  		  	BufferedReader stdError = new BufferedReader(new InputStreamReader(p.getErrorStream()));
  		  	String line = null;
  		  	StringBuffer sb = new StringBuffer(); 
  		  	while ((line = stdInput.readLine()) != null) 
  		  	{  
  		  		sb.append(line);
  		  	} 
  		  	while ((line = stdError.readLine()) != null) 
  		  	{  
  		  		sb.append(line+"\n");
  		  	} 

  		  	return sb.toString();

    	}
    	catch(IOException e)
    	{
			e.printStackTrace();
    	}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		return null;
    }

    public static String executeShellStdout(String path,String script)
    {
		int CpuNum = Runtime.getRuntime().availableProcessors();
		int MemoryMB = (int)Runtime.getRuntime().maxMemory();		
    	try
    	{
  		  	//String [] cmd = {"/bin/bash" ,String.format("%s %d %d", script,CpuNum,MemoryMB)};
  		  	String [] cmd = {"/bin/bash" ,script};
  		  	ProcessBuilder probuilder = new ProcessBuilder(cmd);
  		  	Map<String, String> env = probuilder.environment();
			//env = System.getenv();
  		  	env.put("HOME", System.getProperty("user.home"));
  		  	probuilder.directory(new File(path));
  		  	Process process = probuilder.start();
  		  	process.waitFor();
  		  	BufferedReader stdInput = new BufferedReader(new InputStreamReader(process.getInputStream()));
  		  	BufferedReader stdError = new BufferedReader(new InputStreamReader(process.getErrorStream()));
  		  	String line = null;
  		  	StringBuffer sb = new StringBuffer(); 
  		  	while ((line = stdInput.readLine()) != null) 
  		  	{  
  		  		sb.append(line);
  		  	} 
  		  	while ((line = stdError.readLine()) != null) 
  		  	{  
  		  		sb.append(line+"\n");
  		  	} 

  		  	return sb.toString();

    	}
    	catch(IOException e)
    	{
			e.printStackTrace();
    	}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		return null;
    	
    }

	public static void deleteHdfsPath(FileSystem fs,Path path) throws IOException
	{
		if (fs.exists(path))
		{
			fs.delete(path, true);
		}

	}
  
	/**
	 * split BAM by chromosomes. A.bam -> A.chr1.bam, A.chr2.bam, ...
	 * @throws Exception 
	 * 
	 * */
	private static void splitBamCmd(String fileInputRawBam,String pathOutput) throws Exception
	{
		throw new Exception("Not implemented");
		//String script = String.format("%s/util/%s %s",LocalConfiguration.pathAppDN, "splitBamByChr",LocalConfiguration.pathSamtools);
		//String cmd = String.format("%s %s %s",script,fileInputRawBam,pathOutput);
		//execute(cmd);
	}
	
	/**
	 * split multiple BAMs in parallel
	 * 
	 * */
	private static void splitBams(File[] fileInputRawBams,final String pathOutput)
	{
		class Splitter implements Runnable
		{

			private String name;

			public Splitter(File filename)
			{
				name = filename.getAbsolutePath();
			}

			public void run()
			{
				try
				{
					splitBamCmd(name,pathOutput);
				}
				catch (Exception e)
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		ExecutorService pool2 = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		for (File bam : fileInputRawBams)
		{
			pool2.execute(new Splitter(bam));
		}
		pool2.shutdown();
		try
		{
			pool2.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		}
		catch (InterruptedException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void splitBam(File pathInputRawBam,String pathOutput) 
			throws Exception
	{
		
		if(pathInputRawBam.isDirectory())
		{
			//split in parallel
			File[] files = pathInputRawBam.listFiles(new FilenameFilter() {
			    public boolean accept(File dir, String name) {
			        return name.toLowerCase().endsWith(".bam");
			    }
			});			
			
			splitBams(files,pathOutput);
		}
		else
		{
			splitBamCmd(pathInputRawBam.getAbsolutePath(),pathOutput);
		}

	}

	
	public static String combineVcfFile(Path pathSrc)
	{
		/**
		 * #!/usr/bin/python
import argparse,itertools,glob,os,re,subprocess
if __name__=='__main__':
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("-k","--gatk", help="The full path to GATK's jar file")
    parser.add_argument("-g","--genome", help="The full path to reference genome FASTA file")
    parser.add_argument("-m","--merge", help="The merge oprion, could be UNIQUIFY, ")
    parser.add_argument("-d","--delete",action="store_false", help="delete the original files after merging")
    args = parser.parse_args()

    """

    parser = argparse.ArgumentParser()
    parser.add_argument("INPUT",help="The input path to splitted VCF files")
    parser.add_argument("OUTPUT",help="The output path to merged VCF files")
    args = parser.parse_args()
    chromosomes = [0]*25
    fileNamePattern=re.compile('(.*).chr([0-9xymXYM]).vcf$')
    for root,dirs,files in os.walk(args.INPUT):
        for f in files:
            m = fileNamePattern.match(f)
            if m:
                sampleName,chrIndex = m.groups(0)
                #print chrIndex
                ff = os.path.join(root,f)
                if chrIndex.upper()=='X':
                    chromosomes[22] = ff
                elif chrIndex.upper()=='Y':
                    chromosomes[23] = ff
                elif chrIndex.upper()=='M':
                    chromosomes[24] = ff
                else:
                    chromosomes[int(chrIndex)-1] = ff
    input_Vcfs = ' --variant '+' --variant '.join(list(filter(lambda x: x!=0,chromosomes)))
    #mem_mb= psutil.avail_phymem()/(1024*1024)
    mem_mb= 1024
    path_to_gatk="~/bio/app/gatk/3.1-1/GenomeAnalysisTK.jar"
    path_to_ref="~/bio/data/fasta/hg19/hg19.fa"
    merge_option = 'UNIQUIFY'
    vcf_out = "%s.vcf" % sampleName
    cmd = "java -Xms%dm -jar %s -T CombineVariants -R %s -genotypeMergeOptions %s %s -o %s/%s" % (mem_mb,path_to_gatk, path_to_ref,merge_option,input_Vcfs,args.OUTPUT,vcf_out)
    subprocess.check_call(cmd,shell=True)

		 * 
		 * */
		return "";
	}
}
