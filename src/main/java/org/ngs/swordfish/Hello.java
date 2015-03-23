package org.ngs.swordfish;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;

public class Hello {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
    	String path = "/user/hadoop/hadoop_jobs/22995/";
		String hdfsBasePath = FilenameUtils.normalizeNoEndSeparator(path);
    	System.out.println(StringUtils.replaceOnce(hdfsBasePath,"/user","/home"));

	}

}
