package com.bskyb.cbs.nft.focus.jenkins.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bskyb.cbs.nft.focus.jenkins.influx.InfluxWriter;


public class OracleTablespaceMonitorFreeSpaceDevMonitoringTactical extends OracleMonitor 
{
    private static Logger logger = LoggerFactory.getLogger(OracleTablespaceMonitorFreeSpaceDevMonitoringTactical.class);

	// We've been having problems with the database connection hanging
	// so we've added some timeouts to prevent this.
	// These timeouts are in seconds.
	public static final int QUERY_TIMEOUT = 30;
	public static final int LOGIN_TIMEOUT = 30;
	public int index = 0;
	public InfluxWriter writer = null;
	

	private void handleTablespaces(OracleDB db, ResultSet resultSet, String environment) throws Exception {
		while (resultSet.next()) 
		{
			String tablespace = resultSet.getString(1);
			String pctFree = resultSet.getString(4);
			Integer freeMB = resultSet.getInt(2);
			Integer totalMB = resultSet.getInt(3);

			if (! db.isTableSpaceExcluded(tablespace)) 
			{
				String message = "tablespace,name=" + db.getName() + "." + tablespace + " totalmb=" + totalMB + ",freemb=" + freeMB + ",freepct=" + pctFree;
				logger.info("Writing data for {}.{}", db.getName(), tablespace);
				InfluxWriter.writeData(message);
			}   
		}			
	}

	@Override
	protected void processDatabase(OracleDB db, Boolean handleTablespaces) 
	{
		logger.info("Processing {}, tns={} ...", db.getName(), db.getTNS());
		
		String jdbcDriver = "oracle.jdbc.driver.OracleDriver";
		String jdbcURL = "jdbc:oracle:thin:@" + db.getTNS();
		int dbAvailable = 1;

		try 
		{
			Class.forName(jdbcDriver);
			DriverManager.setLoginTimeout(LOGIN_TIMEOUT);
			
			Connection conn = DriverManager.getConnection(jdbcURL, db.getUsername(), db.getPassword());
			try 
			{
				Statement st = conn.createStatement();
				st.setQueryTimeout(QUERY_TIMEOUT);
				try 
				{
					// Only actually query the DB if we want tablespace information, otherwise a connection is sufficient for availability
					if (handleTablespaces)
					{
				  	  ResultSet resultSet = st.executeQuery(query);
					  try 
				  	  {
						handleTablespaces(db,resultSet, environment);
					  } 
					  finally 
					  {
						resultSet.close();
					  }
					}
				} 
				catch (Exception e)
				{ 
					logger.error("{}", e.getMessage());
				}
				finally 
				{
					st.close();
				}
			} 
			finally 
			{
				conn.close();
			}
		} 
		catch (Exception e) 
		{
			dbAvailable = 0 ;
			logger.error("{}", e.getMessage());
		}
		
		if (1 != dbAvailable)
			logger.error("Database not available {}", db.getName());

	}
	

	public static void main(String[] args) {
		logger.info("QUERY TIMEOUT      = {} seconds", Integer.toString(OracleTablespaceMonitorFreeSpaceDevMonitoringTactical.QUERY_TIMEOUT));
		logger.info("CONNECTION TIMEOUT = {} seconds", Integer.toString(OracleTablespaceMonitorFreeSpaceDevMonitoringTactical.LOGIN_TIMEOUT));
		OracleTablespaceMonitorFreeSpaceDevMonitoringTactical t = new OracleTablespaceMonitorFreeSpaceDevMonitoringTactical();

		if (t.parseCommandLine(args)) 
		{
			if (InfluxWriter.initialiseClient(t.influxURL, t.influxOrg, t.influxBucket, t.influxToken))
			{
				t.run();
				InfluxWriter.shutdownClient();
			}
			else
				logger.error("Failed to initialise connection to InfluxDB, please check supplied parameters");
		}
	}
}




