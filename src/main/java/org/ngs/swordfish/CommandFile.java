package org.ngs.swordfish;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

import org.apache.commons.httpclient.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class CommandFile
{
	/**
	 * parse the command file and get the list of input files appeared in the command file.  
	 * 
	 * @param commandFile: the command file
	 * @param allFiles: list of all files under current working directory
	 * */
	public static List<String> getFiles(Path commandFile,List<String> allFiles)
	{
		List<String> candidateFiles = new ArrayList<String>();
		try
		{
			Configuration conf = new Configuration();
			FileSystem fileSystem = FileSystem.get(commandFile.toUri(), conf);
			BufferedReader bufferedReader = new BufferedReader(
					new InputStreamReader(fileSystem.open(commandFile)));
			
			String line=null;
	        while((line=bufferedReader.readLine())!=null)
	        {
				for(String s : line.split(" "))
				{
					if(!s.startsWith("-"))
					{
						candidateFiles.add(s);
					}
				}
	        }
	        bufferedReader.close();	
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		candidateFiles.retainAll(allFiles);
		//remove duplicates as some files may appear in a script multiple times
		return new ArrayList<>(new LinkedHashSet<>(candidateFiles));		
	}
}
