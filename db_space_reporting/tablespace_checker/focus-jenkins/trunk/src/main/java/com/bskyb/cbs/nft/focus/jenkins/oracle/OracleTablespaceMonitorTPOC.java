package com.bskyb.cbs.nft.focus.jenkins.oracle;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class OracleTablespaceMonitorTPOC extends OracleTablespaceMonitor 
{
	private String envLocation;

	@Override
	protected boolean getDBList(String filename)
	{
		try
		{
			System.out.println(envLocation);
			JSONObject env = getEnvironment().getJSONObject("environment");
			JSONArray applications = env.getJSONArray("applications");
			
			for(int i = 0; i < applications.length(); i++)
			{
				JSONObject app = applications.getJSONObject(i);
				if(app.has("databases"))
				{
					JSONArray dbs = app.getJSONArray("databases");
					for(int j = 0; j < dbs.length(); j++)
					{
						JSONObject db = dbs.getJSONObject(j);
						String sid = db.getJSONObject("databaseInfo").getString("sid");
						String tns = db.getString("tns");
						databases.add(new OracleDB(sid, tns, "HP_DIAG", "HP_DIAG_1234", "UNDOTBS",-1.0));
					}
				}
			}
		}
		catch(JSONException e)
		{
			e.printStackTrace();
			System.out.println("ERROR : Problems parsing the model.");
			return false;
		}
		
		return true;
	}

	@Override
	protected boolean parseCommandLine(String[] args) 
	{
		boolean status = false;
		
		Options options = new Options();
		options.addOption("s",true,"Path to file containing SQL to execute.");
		options.addOption("m",true,"URL for the Environment Model");

		CommandLineParser parser = new PosixParser();

		try 
		{
			CommandLine cmd = parser.parse( options, args);

			if (cmd.hasOption("s") && cmd.hasOption("m")) 
			{
				queryFile=cmd.getOptionValue("s");
				envLocation=cmd.getOptionValue("m");
				status = true;	
			} 
			else 
			{
				System.out.println("ERROR: Must specify database list and query file.");
			}

		} catch (ParseException e) 
		{
			System.out.println("ERROR: Failed to parse command line '" + e.getMessage() + "'");
		}
		
		return status;
	}
	
	private JSONObject getEnvironment()
	{
		JSONObject result = null;
		
		try
		{							
			CloseableHttpClient httpclient = HttpClients.createDefault();
			
			try
			{
				CloseableHttpResponse response = httpclient.execute(new HttpGet(envLocation));
				
				try 
	            {
	                result = new JSONObject(EntityUtils.toString(response.getEntity()));
	            } 
	            finally 
	            {
	                response.close();
	            }
	        } 
	        finally 
	        {
	            httpclient.close();
	        }
		}
		catch(Exception ex)
		{
			System.out.println("ERROR : Could not get JSON from Environment @ " + envLocation);
		}
		
		return result;
	}

	public static void main(String[] args) {

		OracleTablespaceMonitorTPOC t = new OracleTablespaceMonitorTPOC();
		if (t.parseCommandLine(args)) {
			t.run();
		}
	}
}




