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
import org.apache.commons.exec.LogOutputStream;
import org.apache.commons.exec.PumpStreamHandler;
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

	//static final Logger logger = LogManager.getLogger();

    /** Returns null if it failed for some reason.
     */
    public static String command(final String directory,final String cmdline) 
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
            
            if (0 != process.waitFor()) //timeout?
                return null;

            return StringUtils.join(output,"\n");

        } catch (Exception e) {
        	//TODO:
            //present appropriate error messages to the user.
            return null;
        }
    }

    public static String command(final String cmdline) 
    {
    	return command(".",cmdline); 
    }

    public static String script(final String directory,final String script) 
    {
    	return command(directory,"bash "+script); 
    }

    
	/**
	 * run a shell script and capture it's return code as return value.
	 * standard output and standard error were redirected by logger.
	 * 
	 * */
	public static int runScript(String path,String script) throws Exception 
	{
        CommandLine oCmdLine = CommandLine.parse("bash "+script);
        DefaultExecutor oDefaultExecutor = new DefaultExecutor();
        oDefaultExecutor.setExitValue(0);
        oDefaultExecutor.setWorkingDirectory(new File(path));
        //oDefaultExecutor.setStreamHandler(new PumpStreamHandler(new ExecLogHangler(logger, Level.ERROR)));
        oDefaultExecutor.setStreamHandler(new PumpStreamHandler(new CollectingLogOutputStream()));
        return oDefaultExecutor.execute(oCmdLine);
    }

	/**
	 * run a command and capture it's output as return value
	 * 
	 * */
	public static String runCommand(String command) throws Exception
	{
        CommandLine oCmdLine = CommandLine.parse(command);
        DefaultExecutor oDefaultExecutor = new DefaultExecutor();
        oDefaultExecutor.setExitValue(0);
        CollectingLogOutputStream output = new CollectingLogOutputStream();
        oDefaultExecutor.setStreamHandler(new PumpStreamHandler(output));
        oDefaultExecutor.execute(oCmdLine);
        return  StringUtils.join(output.getLines(),"\n"); 
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
	
	@Deprecated
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

	@Deprecated
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
	public static void main(String[] argv)
	{
		
		try {
			Util.runScript("/home/hadoop/0000","1.cmd");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
}
