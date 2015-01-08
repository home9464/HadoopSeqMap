package org.ngs.swordfish;
import java.io.IOException;
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
    static List<String> datanodes = new ArrayList<>();
    static
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
    public static List<String> getDatanodes()
    {
    	return datanodes;
    }
	private static int getMemoryMb(String[] commands)
	{
		int MB = 1024;
		int mem_MB = 4096; //default
		//use "cat /proc/meminfo"
		Pattern pattern = Pattern.compile("^MemTotal:\\s+(\\d+)\\s*kB");
		Matcher matcher;
		try 
		{
			matcher = pattern.matcher(Util.runCommand(commands[0]));
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
			matcher = pattern.matcher(Util.runCommand(commands[1]));
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
			matcher = pattern.matcher(Util.runCommand(commands[2]));
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
	
    /**
     * Get the available memory in MB of NameNode
     * */
	public static int getMemoryMbNN()
	{
		String[] commands={"cat /proc/meminfo","vmstat -s","free -m"};
		return getMemoryMb(commands);
		
	}

    /**
     * Get the available memory in MB of DataNode
     * */
	public static int getMemoryMbDN()
	{
		if(datanodes.size()>0)
		{
			String dn1 = datanodes.get(0);
			String[] commands={String.format("ssh %s \"cat /proc/meminfo \" 2>/dev/null",dn1),
				String.format("ssh %s \"vmstat -s\" 2>/dev/null",dn1),
				String.format("ssh %s \"free -m\" 2>/dev/null",dn1)
				};
			return getMemoryMb(commands);
		}
		return 4096;
	}	
	

    /**
     * Get the number of CPU cores of NameNode
     * */
	public static int getNumCpuCore()
	{
		String command = "grep -c ^processor /proc/cpuinfo 2>/dev/null";
		try 
		{
			return Integer.parseInt(Util.runCommand(command));
		} 
		catch (Exception e) 
		{
			return 1;
		}
	}
    /**
     * Get the number of CPU cores of each DataNode
     * */
	public static int getNumCpuCoreDN()
	{
		try
		{
			String dn1 = datanodes.get(0);
			String command = String.format("ssh -t %s \"grep -c ^processor /proc/cpuinfo\" 2>/dev/null",dn1);
			return Integer.parseInt(Util.runCommand(command));
		}
		catch (Exception e) 
		{
			return 1;
		}
		
	}
	
    /**
     * Get the number of DataNodes
     * */
	public static int getNumDN()
	{
		return datanodes.size();
	}
	public static void main(String[] argv)
	{
		getMemoryMbNN();
	}
	
}
