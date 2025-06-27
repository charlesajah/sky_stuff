package com.bskyb.cbs.nft.focus.jenkins.oracle;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public abstract class OracleMonitor 
{
	String queryFile=null; 
	String dbListSrc=null;
	boolean useModel=false;
	Boolean handleTablespaces = true;
	String dbList="";
	String query="";
	Double minPercent=(double) 5;
	String environment="";
	ArrayList<OracleDB> databases = new ArrayList<OracleDB>(0);
	String influxURL = "";
	String influxOrg = "";
	String influxBucket = "";
	String influxToken = "";
	
	protected boolean getDBList(String src)
	{
		if (!useModel)
			return getDBListFromFile(src);
		else
			return getDBListFromModel(src);
	}
	
	private boolean getDBListFromFile (String filename)
	{
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			try {
				String line = br.readLine();
				while (line != null) {
					if ((! line.contains("#") && line.contains(","))) {
						line=line.trim();
						int count = StringUtils.countMatches(line, ",");
						
						String[] fields = line.split(",",count+1);
						if (count == 5)
						  databases.add(new OracleDB(fields[0],fields[1],fields[2],fields[3],fields[4],Double.parseDouble(fields[5])));
						else
						  databases.add(new OracleDB(fields[0],fields[1],fields[2],fields[3],fields[4],-1.0));
					}
					line = br.readLine();
				}
			} finally {
				br.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR: Failed to get list of databases to process from '" + filename + "'");
			return false;
		}
		
		return true;
	}

	private boolean getDBListFromModel (String url)
	{
		// This method returns the databases member with databases retrieved from the environment model pointed to by the url.
		// Only support for the Neo4j environment model has been implemented.
		
		try
		{
			JSONArray dbArray = getModelDatabases(url);
			
			for(int i = 0; i < dbArray.length(); i++)
			{
				JSONObject db = dbArray.getJSONObject(i);
				if((db.has("tns")) && (db.has("sid")))
				{
					String tns = db.getString("tns");
					String sid = db.getString("sid");
					databases.add(new OracleDB(sid, tns, "HP_DIAG", "HP_DIAG_1234", "UNDOTBS",-1.0));
				}
			}
		}
		catch(JSONException e)
		{
			e.printStackTrace();
			System.out.println("ERROR : Problems parsing model response.");
			return false;
		}
		
		return true;
	}

	private JSONArray getModelDatabases(String url)
	{
		JSONArray result = null;
		try
		{							
			CloseableHttpClient httpclient = HttpClients.createDefault();
			try
			{
				CloseableHttpResponse response = httpclient.execute(new HttpGet(url));
				try 
	            {
	                result = new JSONArray(EntityUtils.toString(response.getEntity()));
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
			System.out.println("ERROR : Could not get JSON from Environment Model @ " + url);
		}
		
		return result;
	}

	protected boolean getSQL(String filename)
	{
		try {
			BufferedReader br = new BufferedReader(new FileReader(filename));
			try {
				String line = br.readLine();
				while (line != null) {
					query = query + line + " ";
					line = br.readLine();
				}
			} finally {
				br.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("ERROR: Failed SQL from '" + filename + "'");
			return false;
		}
		
		return true;
	}
	
	protected boolean parseCommandLine(String[] args) 
	{
		//OracleTablespaceMonitor -s sqlFile -d dbList
		boolean status = false;
		
		Options options = new Options();
		options.addOption("s",true,"Path to file containing SQL to execute.");
		options.addOption("d",true,"Path to file containing list of database TNS sting to process.");
		options.addOption("m",true,"URL that returns list of databases from environment model.");
		options.addOption("n",false,"Disable tablespace reporting.");
		options.addOption("p",true, "Minimum percent free");
		options.addOption("e",true, "The environment being processed.");
		options.addOption("i", true, "Influx DB URL");
		options.addOption("b", true, "Influx DB bucket");
		options.addOption("o", true, "Influx DB organisation");
		options.addOption("t", true, "Influx DB token");
		
		

		CommandLineParser parser = new PosixParser();

		try {
			CommandLine cmd = parser.parse( options, args);

			if (cmd.hasOption("s") && cmd.hasOption("d")) {
				queryFile=cmd.getOptionValue("s");
				dbListSrc=cmd.getOptionValue("d");
				useModel = false;
				status = true;
				
			} else if (cmd.hasOption("s") && cmd.hasOption("m")) {
				queryFile=cmd.getOptionValue("s");
				dbListSrc=cmd.getOptionValue("m");
				useModel = true;
				status = true;
			} else {
				System.out.println("ERROR: Must specify query file and either a database list or a URL to a model that produces a database list.");
			}
			
			if (cmd.hasOption("n"))
				handleTablespaces = false;
			
			if (cmd.hasOption("p"))
				minPercent = Double.parseDouble(cmd.getOptionValue("p"));
			
			if (cmd.hasOption("e"))
				environment = cmd.getOptionValue("e");
			
			if (cmd.hasOption("i"))
				influxURL = cmd.getOptionValue("i");
			
			if (cmd.hasOption("b"))
				influxBucket = cmd.getOptionValue("b");
			
			if (cmd.hasOption("o"))
				influxOrg = cmd.getOptionValue("o");
			
			if (cmd.hasOption("t"))
				influxToken = cmd.getOptionValue("t");

			
		} catch (ParseException e) {
			System.out.println("ERROR: Failed to parse command line '" + e.getMessage() + "'");
		}
		
		return status;
	}
	
	protected void run()
	{
		if (getSQL(queryFile) && getDBList(dbListSrc)) 
		{
			String timeoutClass = "oracle.jdbc.driver.OracleTimeoutThreadPerVM";
			try
			{
				// Load this class before any JDBC operations have been attempted in order to prevent
				// the failure to close the oracle.jdbc.deiver.OracleTimeoutPollingThread when run as a Jenkins job.
				// (See http://java.jiderhamn.se/2012/02/26/classloader-leaks-v-common-mistakes-and-known-offenders/)
				Class.forName(timeoutClass);
			}
			catch (Exception e)
			{
				System.out.println("ERROR: Failed to load " + timeoutClass);
			}
			for (OracleDB db : databases) 
			{
				processDatabase(db, handleTablespaces);
			}
		}
	}
	
	protected abstract void processDatabase(OracleDB db, Boolean handleTablespaces);
}
