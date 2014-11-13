
package org.ngs.swordfish;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class InputSplitterTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public InputSplitterTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( InputSplitterTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testlistFiles()
    {
    	InputSplitter splitter = new InputSplitter("/home/hadoop/tmp","/home/hadoop/tmp/splitted");
    	try {
			splitter.split();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}
