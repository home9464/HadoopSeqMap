package org.ngs.swordfish;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
public class test
{
	
	public static void main(String[] args) throws Exception
	{
		
		HttpClient httpClient = new HttpClient();

		String _url = "https://clownfish:utah20zer07@192.168.135.128:5050/api/v1/jobs/2015-05-16-01-34-12-572d23ec32be9499";
		PutMethod put = new PutMethod(_url);
		StringRequestEntity entity=new StringRequestEntity("HELLO",
				"application/xml",
				"UTF-8");
		put.setRequestEntity(entity);
		httpClient.executeMethod(put);
		  
		//put.setRequestBody(data);		
		
	    //put.setRequestBody(new FileInputStream("UploadMe.gif"));
		//HttpPut putRequest = new HttpPut(URI);

		//StringEntity input = new StringEntity(XML);
		//input.setContentType(CONTENT_TYPE);

	}
}
