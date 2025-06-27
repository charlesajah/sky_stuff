package com.bskyb.cbs.nft.focus.jenkins.oracle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class OracleExecution extends OracleMonitor
{
	@Override
	protected void processDatabase(OracleDB db, Boolean handleTablespaces) 
	{
		System.out.println("NOTE: Processing " + db.getName() + "...");
		String jdbcDriver = "oracle.jdbc.driver.OracleDriver";
		String jdbcURL = "jdbc:oracle:thin:@" + db.getTNS();
		int dbAvailable = 1;

		Statement st = null;
		Connection conn = null;
		try 
		{
			Class.forName(jdbcDriver);
			conn = DriverManager.getConnection(jdbcURL, db.getUsername(), db.getPassword());
			
			st = conn.createStatement();
			ResultSet rs = st.executeQuery(query);
										
			while (rs.next()) 
			{
				System.out.println(rs.getString(1));
			}
		} 
		catch (Exception e) 
		{
			dbAvailable = 0 ;
			System.out.println("ERROR: " + e.getMessage());
		}
		finally
		{
			try
			{
				if(st != null) { st.close(); }
				if(conn != null) { conn.close(); }
			}
			catch (SQLException e)
			{	
				System.out.println("ERROR: " + e.getMessage());
			}
		}
		System.out.println("_datapoint:DatabaseAvailability:" + db.getName() + " Availability:status:" + dbAvailable);
	}
	
	public static void main(String[] args) 
	{
		OracleExecution osm = new OracleExecution();
		if (osm.parseCommandLine(args)) 
		{
			osm.run();
		}
	}
}
