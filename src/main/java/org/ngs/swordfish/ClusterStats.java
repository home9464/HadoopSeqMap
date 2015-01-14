package org.ngs.swordfish;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ClusterStats
{
	private static ClusterStats instance=null;
    private List<String> datanodes = new ArrayList<>();
	private ClusterStats()
	{
		getDataNodes();
	}
	private void getDataNodes()
	{
		Configuration conf = new Configuration(); 
		try
		{
			YarnClient	yarnClient = YarnClient.createYarnClient();
			yarnClient.init(conf);
			yarnClient.start();
			//YarnClusterMetrics metrics = yarnClient.getYarnClusterMetrics();
		    //YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
			//numDataNodes = clusterMetrics.getNumNodeManagers();
			
		    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
		    //numDataNodes = clusterNodeReports.size();
		    //LOG.info("Got Cluster node info from ASM");
		    for (NodeReport node : clusterNodeReports) 
		    {
		    	//System.out.println(node.getNodeId().getHost());
		    	datanodes.add(node.getNodeId().getHost());
		    	//Resource res = node.getCapability();
		    	//numCores = res.getVirtualCores();
		    	//mbMem = res.getMemory();
		    	//System.out.println("Got node report from ASM for"
		        //  + ", nodeId=" + node.getNodeId() 
		        //  + ", nodeAddress" + node.getHttpAddress()
		        //  + ", nodeRackName" + node.getRackName()
		        //  + ", nodeNumContainers" + node.getNumContainers());
		    }
			//String cmd = "cat /proc/meminfo  | grep \"MemTotal\" | awk '{print $2}'";
			//Util.executeStdout(cmd);
		}
		catch (YarnException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static ClusterStats getInstance()
	{
		if(instance ==null)
			return new ClusterStats();
		return instance;
	}

    private int getMemoryMb(String host)
	{
		int MB = 1024;
		int mem_MB = 7168; //default, 7GB
		String command = "cat /proc/meminfo";
		Pattern pattern = Pattern.compile("^MemTotal:\\s+(\\d+)\\s*kB");
		Matcher matcher;
		try 
		{
			if (host != null)
			{
				command = String.format("ssh %s \"%s\"",host,command);
			}
			matcher = pattern.matcher(Util.command(command));
			if (matcher.find()) 
			{
				return Integer.parseInt(matcher.group(1))/MB;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.err.println(command);
			e.printStackTrace();
		}
		return mem_MB;
	}

    private int getNumCpuCores(String host)
	{
		String command = "grep -c ^processor /proc/cpuinfo";
		try 
		{
			if (host != null)
			{
				command = String.format("ssh %s \"%s\"",host,command);
			}
			return Integer.parseInt(Util.command(command));
		} 
		catch (Exception e) 
		{
			e.printStackTrace();
			return 1;
		}
	}

    @Deprecated
    private int getMemoryMb2(String[] commands)
	{
		int MB = 1024;
		int mem_MB = 7168; //default, 7GB
		//use "cat /proc/meminfo"
		Pattern pattern = Pattern.compile("^MemTotal:\\s+(\\d+)\\s*kB");
		Matcher matcher;
		try 
		{
			matcher = pattern.matcher(Util.command(commands[0]));
			if (matcher.find()) 
			{
				return Integer.parseInt(matcher.group(1))/MB;
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//use "vmstat -s"
		
		pattern = Pattern.compile("\\s*(\\d+)\\s+.*total memory");
		try {
			matcher = pattern.matcher(Util.command(commands[1]));
			if (matcher.find()) 
			{
				return Integer.parseInt(matcher.group(1))/MB; 
			}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//use "free -m"
		pattern = Pattern.compile(".*Mem:\\s*(\\d+)\\s+.*");
		try {
			matcher = pattern.matcher(Util.command(commands[2]));
			if (matcher.find()) 
			{
				return Integer.parseInt(matcher.group(1)); 
			}			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return mem_MB;
	}
	
	
    public List<String> getDatanodes()
    {
    	return datanodes;
    }

    /**
     * Get the available memory in MB of NameNode
     * */
	public int getMemoryMbNN()
	{
		return getMemoryMb(null);
	}


    /**
     * Get the available memory in MB of DataNode
     * */
	public int getMemoryMbDN()
	{
		return getMemoryMb(datanodes.get(0));
	}	
	

    /**
     * Get the number of CPU cores of NameNode
     * */
	public int getNumCpuCoresNN()
	{
		return this.getNumCpuCores(null);
	}
	
    /**
     * Get the number of CPU cores of each DataNode
     * */
	public int getNumCpuCoresDN()
	{
		return this.getNumCpuCores(datanodes.get(0));
	}
	
    /**
     * Get the number of DataNodes
     * */
	public int getNumDN()
	{
		return datanodes.size();
	}
	
	public static void main(String[] argv)
	{
		System.out.println("HELLO");
		try
		{
			System.out.println(Util.command("/home/hadoop/0000/","2.sh"));
		}
		catch (Exception e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ClusterStats.getInstance().getMemoryMbDN();
	}
}
