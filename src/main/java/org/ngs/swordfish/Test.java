package org.ngs.swordfish;


public class Test 
{
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//logger.error("HELLO");
    	String path = "/home/hadoop/chr13";
    	String script = "1.cmd";
    	//Util.runScript(path,script);
    	//Util.runCommand("hello");
    	try {
			System.out.println(Util.runCommand("date"));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}