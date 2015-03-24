package org.ngs.swordfish;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.exec.DefaultExecutor;
import org.apache.commons.exec.ExecuteException;
import org.apache.commons.exec.ExecuteWatchdog;
import org.apache.commons.exec.Executor;
import org.apache.commons.io.FileUtils;

public class Hello {

	public static void main(String[] args) throws ExecuteException, IOException {
		// TODO Auto-generated method stub
    	//String path = "/user/hadoop/hadoop_jobs/22995/";
		//String hdfsBasePath = FilenameUtils.normalizeNoEndSeparator(path);
    	//System.out.println(StringUtils.replaceOnce(hdfsBasePath,"/user","/home"));
    	runScript2("/home/hadoop/hello.sh");
    	

	}
	
	public static int runScript(String scriptFile) throws ExecuteException, IOException
	{
		File scriptDir = new File(FilenameUtils.getFullPath(scriptFile)+"/");
		//System.out.println(scriptDir);
		String scriptName = FilenameUtils.getName(scriptFile);
		System.out.println(scriptName);
		CommandLine cmd = new CommandLine(scriptName);
		Executor exec = new DefaultExecutor();
		exec.setWorkingDirectory(scriptDir);
		return exec.execute(cmd);
	}
	public static int runScript2(String scriptFile)
	{
		//File scriptDir = new File(FilenameUtils.getFullPath(scriptFile));
		File scriptDir = new File("/home/hadoop/tmp");
		CommandLine oCmdLine = new CommandLine("/bin/bash "+scriptFile);
	    DefaultExecutor defaultExecutor = new DefaultExecutor();
	    defaultExecutor.setExitValue(0);
	    ExecuteWatchdog watchdog = new ExecuteWatchdog(120000);
	    defaultExecutor.setWatchdog(watchdog);
	    
	    defaultExecutor.setWorkingDirectory(scriptDir);
	    try 
	    {
	    	return defaultExecutor.execute(oCmdLine);
	    } 
	    catch (ExecuteException e) 
	    {
	    	System.err.println("Execution failed.");
	        e.printStackTrace();
	    } 
	    catch (IOException e) 
	    {
	    	System.err.println("permission denied.");
	        e.printStackTrace();
	    }
		return 0;
	}
	
	public static int runScript3(String scriptFile)
	{
		String[] cmd = { "/bin/sh", "-c", scriptFile};
		BufferedReader bri = null, bre = null;
		int exitC = 0;
		try 
		{
			Process p = Runtime.getRuntime().exec(cmd);
			exitC = p.waitFor();
			bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
			bre = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			String line = "";
			while ((line = bri.readLine()) != null) 
			{
				System.out.println(line);               
			}
			while ((line = bre.readLine()) != null) 
			{
				System.out.println(line);
			}
			bri.close();
			bre.close();
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
		} 
		return exitC;
	}

}
