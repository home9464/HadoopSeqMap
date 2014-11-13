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

	public static String getCommonPrefix(String s1,String s2)
	{
		String[] sz = new String[2];
		sz[0]=s1;
		sz[1]=s2;
		return getCommonPrefix(sz);
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

	
}
