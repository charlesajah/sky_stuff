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

public class OracleTablespaceMonitorFreeSpaceNonFocus extends OracleMonitor 
{
	// We've been having problems with the database connection hanging
	// so we've added some timeouts to prevent this.
	// These timeouts are in seconds.
	public static final int QUERY_TIMEOUT = 30;
	public static final int LOGIN_TIMEOUT = 30;

	private void handleTablespaces(OracleDB db, ResultSet resultSet) throws Exception {
		while (resultSet.next()) {
			String tablespace = resultSet.getString(1);
			String pctFree = resultSet.getString(4);

			if (! db.isTableSpaceExcluded(tablespace)) 
			{
				Double checkValue = minPercent;
				if (db.getPercent() > 0.0)
					checkValue = db.getPercent();
				if (Double.parseDouble(pctFree) < checkValue)
				    System.out.println("_datapoint:DatabaseTablespace:" + db.getName() + "_" + tablespace + ":freepct:" + pctFree);
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
						handleTablespaces(db,resultSet);
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
		System.out.println("_datapoint:DatabaseAvailability:" + db.getName() + " Availability:status:" + dbAvailable);
       // Date endTime = new Date();
		//System.out.println("NOTE: Processed in " + (((double)(endTime.getTime()) - startTime.getTime()) / 1000) + " second(s).");	
	}

	public static void main(String[] args) {
		System.out.println("NOTE: QUERY TIMEOUT      = " + Integer.toString(OracleTablespaceMonitorFreeSpaceNonFocus.QUERY_TIMEOUT) + " seconds");
		System.out.println("NOTE: CONNECTION TIMEOUT = " + Integer.toString(OracleTablespaceMonitorFreeSpaceNonFocus.LOGIN_TIMEOUT) + " seconds");
		OracleTablespaceMonitorFreeSpaceNonFocus t = new OracleTablespaceMonitorFreeSpaceNonFocus();
		if (t.parseCommandLine(args)) {
			System.out.println("NOTE: MINIMUM FREE PERCENT  = " + t.minPercent);
			t.run();
		}
	}
}




