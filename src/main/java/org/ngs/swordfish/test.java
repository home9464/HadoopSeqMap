package org.ngs.swordfish;

import java.text.SimpleDateFormat;
import java.util.Date;

public class test
{
	
	public static void main(String[] args) throws Exception
	{
		
		SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH-mm-ss");//dd/MM/yyyy
	    String strDate = sdfDate.format(new Date());
	    System.out.println(strDate);
	}
}
