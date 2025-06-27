package com.bskyb.cbs.nft.focus.jenkins.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
 
//public class GetCurrentTimeStamp 
//{
//    public static void main( String[] args )
//    {
//	 java.util.Date date= new java.util.Date();
//	 System.out.println(new Timestamp(date.getTime()));
//    }
//}

public class OracleTablespaceMonitorFreeSpaceHorusTactical extends OracleMonitor 
{
	// We've been having problems with the database connection hanging
	// so we've added some timeouts to prevent this.
	// These timeouts are in seconds.
	public static final int QUERY_TIMEOUT = 30;
	public static final int LOGIN_TIMEOUT = 30;
	public int index = 0;

	private void handleTablespaces(OracleDB db, ResultSet resultSet, String environment) throws Exception {
		while (resultSet.next()) {
			String tablespace = resultSet.getString(1);
			String pctFree = resultSet.getString(4);
			Double pctUsed = 100 - Double.parseDouble(pctFree);

			if (! db.isTableSpaceExcluded(tablespace)) 
			{
				Double checkValue = minPercent;
				if (db.getPercent() > 0.0)
					checkValue = db.getPercent();
				
			   String message = "Database " + db.getName() + ", tablespace " + tablespace  + " is at " + String.format("%.2f", pctUsed) + "% used. Maximum allowed is " + (100-checkValue) + "%"; 

				if (Double.parseDouble(pctFree) < checkValue)
					doHorusEvaluation(index++, "databaseTablespaceFree", db.getName(), tablespace, "CRITICAL", message);
				else
					doHorusEvaluation(index++, "databaseTablespaceFree", db.getName(), tablespace, "OK", message);
				
				
				   // System.out.println("_datapoint:DatabaseTablespace:" + db.getName() + "_" + tablespace + ":freepct:" + pctFree);
			}   
		}			
	}

	@Override
	protected void processDatabase(OracleDB db, Boolean handleTablespaces) 
	{
		System.out.println("NOTE: Processing " + db.getName() + ", tns=" + db.getTNS()+ " ...");
        //Date startTime = new Date();
		
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
					System.out.println("QUERY ERROR: " + e.getMessage());
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
			System.out.println("CONNECTION ERROR: " + e.getMessage());
			//e.printStackTrace();
		}
		
		if (1 == dbAvailable)
			doHorusEvaluation(index++,"databaseAvailability",  db.getName(), "_Availability", "OK", "Database " + db.getName() + " is available.");
		else
			doHorusEvaluation(index++,"databaseAvailability",  db.getName(), "_Availability", "CRITICAL", "Database " + db.getName() + " is not available.");
		//System.out.println("_datapoint:DatabaseAvailability:" + db.getName() + " Availability:status:" + dbAvailable);
       // Date endTime = new Date();
		//System.out.println("NOTE: Processed in " + (((double)(endTime.getTime()) - startTime.getTime()) / 1000) + " second(s).");	
	}
	
	private void doHorusEvaluation(int index, String metricName, String sid, String dsLeaf,String status, String message)
	{
		String base = "_HORUS_EVALUATION;" + environment + ";" + index + ";";
		
		System.out.println(base + "metricTypeName;NonModelledEntityMetric");
		System.out.println(base + "metricName;" + metricName);
		System.out.println(base + "dataStorePattern;Oracle Tactical::" + sid + "::" + dsLeaf);
		System.out.println(base + "documentationLink;https://confluence.bskyb.com/pages/viewpage.action?pageId=184608226");
		System.out.println(base + "status;" + status);
		System.out.println(base + "message;" + message);
		
		
	}
	

	

	public static void main(String[] args) {
		System.out.println("NOTE: QUERY TIMEOUT      = " + Integer.toString(OracleTablespaceMonitorFreeSpaceHorusTactical.QUERY_TIMEOUT) + " seconds");
		System.out.println("NOTE: CONNECTION TIMEOUT = " + Integer.toString(OracleTablespaceMonitorFreeSpaceHorusTactical.LOGIN_TIMEOUT) + " seconds");
		OracleTablespaceMonitorFreeSpaceHorusTactical t = new OracleTablespaceMonitorFreeSpaceHorusTactical();
		if (t.parseCommandLine(args)) {
			System.out.println("NOTE: MINIMUM FREE PERCENT  = " + t.minPercent);
			if (t.environment.equals(""))
			{
			  System.out.println("MUST specify anb environment e.g. N01");
			  System.exit(1);
			}
			t.run();
		}
	}
}




