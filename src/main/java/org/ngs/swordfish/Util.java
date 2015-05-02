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
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

class CollectingLogOutputStream extends LogOutputStream {
    private final List<String> lines = new LinkedList<String>();
    @Override protected void processLine(String line, int level) {
        lines.add(line);
    }   
    public List<String> getLines() {
        return lines;
    }
}
/*
class ExecLogHangler extends LogOutputStream {
    private Logger log;

    public ExecLogHangler(Logger log, Level level) {
        super(level.intLevel());
        this.log = log;
    }

    @Override
    protected void processLine(String line, int level) {
        log.log(Level.forName("Level",level), line);
        //System.out.println(line);
        
    }
}

*/

public class Util
{

    
	/**
	 * run a shell script and capture it's return code as return value.
	 * standard output and standard error were redirected by logger.
	 * 
	 * */
	public static int runScript(String scriptFile) throws ExecuteException, IOException
	{
		File workingDir = new File(FilenameUtils.getFullPathNoEndSeparator(scriptFile));
		
		//change the permission to executable
		CommandLine cmd = CommandLine.parse("chmod +x "+scriptFile);
		Executor executor = new DefaultExecutor();
		executor.setWorkingDirectory(workingDir);
        //executor.setStreamHandler(new PumpStreamHandler(new ExecLogHangler(logger, Level.ERROR)));
        //executor.setStreamHandler(new PumpStreamHandler(new CollectingLogOutputStream()));
		
		//execute the script
		executor.execute(cmd,System.getenv());
		cmd = CommandLine.parse(scriptFile);
		return executor.execute(cmd);
	}
    
	/**
	 * run a command and capture it's output as return value
	 * 
	 * */
	public static String runCommand(String command) throws Exception
	{
        CommandLine cmd = CommandLine.parse(command);
        DefaultExecutor executor = new DefaultExecutor();
        executor.setExitValue(0);
        CollectingLogOutputStream output = new CollectingLogOutputStream();
        executor.setStreamHandler(new PumpStreamHandler(output));
        executor.execute(cmd);
        return  StringUtils.join(output.getLines(),"\n"); 
    }

	/**
	 * run a command and capture it's output as return value
	 * 
	 * */
	public static void postStatus(String statusUrl, String user, String password,String state,String info)
	{
		String content = String.format("-d \"state=%s\" -d \"info=%s\"",state,info);
		
		if (statusUrl != null)
		{
			try
			{
				Util.runCommand(String.format("curl -u %s:%s -X PUT -s %s %s",user,password,content,statusUrl));
			}
			catch(Exception e)
			{
				System.err.println(e);
			}
		}
		else
		{
			System.err.println(content);
		}
    }

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

	/**
	 * get the common prefix of strings
	 * 
	 * { "A_1.txt","A_2.txt" } -> "A_"
	 * */
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

	
	/**
	 * split multiple BAMs in parallel
	 * */
	
	@Deprecated
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
					//splitBamCmd(name,pathOutput);
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
    @Deprecated
    public static String command2(final String directory,final String cmdline) 
    {
        try {
            Process process = 
                new ProcessBuilder(new String[] {"bash", "-c", cmdline})
                    .redirectErrorStream(true)
                    .directory(new File(directory))
                    .start();

            ArrayList<String> output = new ArrayList<String>();
            BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            String line = null;
            while ( (line = br.readLine()) != null )
                output.add(line);
            
            //wait for completion
            int retVal = process.waitFor();
            
            if (0 != retVal) //timeout?
                return "failed: can not wait for porcess to finish "+String.valueOf(retVal);

            return StringUtils.join(output,"\n");

        } catch (Exception e) {
            return "failed: "+e.getMessage();
            //return null;
        }
    }
    
    @Deprecated
    public static String command3(final String cmdline) 
    {
    	return command2(".",cmdline); 
    }

    /** Returns null if it failed for some reason.
     */
    
    @Deprecated
    public static String script4(final String scriptFile) 
    {
        try {
    		File workingDir = new File(FilenameUtils.getFullPathNoEndSeparator(scriptFile));
            Process process = new ProcessBuilder(new String[] {"bash", scriptFile})
                    .redirectErrorStream(true)
                    .directory(workingDir)
                    .start();

            ArrayList<String> output = new ArrayList<String>();
            BufferedReader br = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            String line = null;
            while ( (line = br.readLine()) != null )
                output.add(line);
            
            //wait for completion
            int retVal = process.waitFor();
            
            if (0 != retVal) //timeout?
                return "failed: can not wait for porcess to finish "+String.valueOf(retVal);

            return StringUtils.join(output,"\n");

        } catch (Exception e) {
            return "failed: "+e.getMessage();
            //return null;
        }
    }
}

