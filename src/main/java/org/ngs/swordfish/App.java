package org.ngs.swordfish;

import java.io.IOException;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	//String localInputPath="/home/hadoop/test/output";
		//Configuration conf = new Configuration();
		//FileSystem fs = FileSystem.newInstance(conf);
		//fs.delete(new Path(localInputPath),true);
		System.out.println(System.getProperty("user.home"));
		System.out.println(FilenameUtils.getFullPath("/A/B/C/D/"));
    }
}
