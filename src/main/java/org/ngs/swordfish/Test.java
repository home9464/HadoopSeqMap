package org.ngs.swordfish;

import java.util.UUID;


public class Test 
{
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		//logger.error("HELLO");
    	String path = "/home/hadoop/chr13";
    	if(path.startsWith("/"))
    		System.out.println(path.replaceFirst("/",""));
	}

}
