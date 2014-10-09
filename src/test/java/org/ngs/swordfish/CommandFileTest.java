package org.ngs.swordfish;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class CommandFileTest 
    extends TestCase
{
	
    public CommandFileTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CommandFileTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
    	List<String> allFiles = new ArrayList<>();
    	allFiles.add("A_100k_0000_R1.fastq.gz");
    	allFiles.add("A_100k_0000_R2.fastq.gz");
    	
    	String FileName = "/user/hadoop/job/hadoop@scheduler/1/input/A_100k_0000.cmd";
    	Path p = new Path("hdfs://scheduler:8020/user/hadoop/tests/1.cmd");
    	
    	List<String> ret = CommandFile.getFiles(p,allFiles);
    	for(String r:ret)
    	{
    		System.out.println(r);
    	}
    	assert (ret.size()==5); 
    }
    
}
