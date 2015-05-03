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

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
    	//String path = "/user/hadoop/hadoop_jobs/22995/";
		//String hdfsBasePath = FilenameUtils.normalizeNoEndSeparator(path);
    	//System.out.println(StringUtils.replaceOnce(hdfsBasePath,"/user","/home"));
    	//runScript4("/home/hadoop/0000/cmd.sh");
		String command = "curl -u hello:world -X PUT -s -d \"state=OOOKKeweqwe\" -d \"info=I am fine\" http://192.168.1.16:5000/jobs/May-03-2015-11-24-31-05dfab46d18a3437";
		Util.command3(command);
		//String url="http://192.168.1.16:5000/jobs/May-03-2015-11-24-31-05dfab46d18a3437";
		//Util.putStatus(url,"hello","world","OK","I am fine");
		System.out.println("OK");

	}
	
	public static int runScript(String scriptFile) throws ExecuteException, IOException
	{
		File workingDir = new File(FilenameUtils.getFullPathNoEndSeparator(scriptFile));
		CommandLine cmd = CommandLine.parse("chmod +x "+scriptFile);
		Executor exec = new DefaultExecutor();
		exec.setWorkingDirectory(workingDir);
		exec.execute(cmd);
		cmd = CommandLine.parse(scriptFile);
		return exec.execute(cmd);
	}
	
	public static int runScript4(String scriptFile) throws IOException, InterruptedException
	{
		File workingDir = new File(FilenameUtils.getFullPathNoEndSeparator(scriptFile));
		ProcessBuilder pb = new ProcessBuilder(scriptFile);
		pb.directory(workingDir);
		Process p = pb.start();
		return p.waitFor();
	}
	 
	public static int runScript2(String scriptFile) throws ExecuteException, IOException
	{
		//File scriptDir = new File(FilenameUtils.getFullPath(scriptFile));
		File scriptDir = new File("/home/hadoop/tmp");
		CommandLine oCmdLine = new CommandLine("/bin/bash "+scriptFile);
	    DefaultExecutor defaultExecutor = new DefaultExecutor();
	    defaultExecutor.setExitValue(0);
	    ExecuteWatchdog watchdog = new ExecuteWatchdog(120000);
	    defaultExecutor.setWatchdog(watchdog);
	    defaultExecutor.setWorkingDirectory(scriptDir);
	    return defaultExecutor.execute(oCmdLine);
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
