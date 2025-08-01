package com.bskyb.cbs.nft.focus.jenkins.oracle;

import java.util.ArrayList;

public class OracleDB {
	
	String dbName = "";
	String tnsString = "";
	String username = "";
	String password = "";
	Double percent = 0.0;
	
	ArrayList<OracleTablespace> excludedTablespaces = new ArrayList<OracleTablespace>();
	
	
	public OracleDB(String dbName, String tnsSting, String username, String password, String excludedTablespaces, Double percent)
	{
		this.dbName = dbName;
		this.tnsString = tnsSting;
		this.username = username;
		this.password = password;
		this.percent = percent;
		
		String[] ex = excludedTablespaces.split(":");
		for (String ts : ex) {
			this.excludedTablespaces.add(new OracleTablespace(ts));
		}
	}
	
	
	public String getName() {
		return dbName;
	}

	public String getTNS() {
		return tnsString;
	}
	
	public String getUsername() {
		return username;
	}
	
	public String getPassword() {
		return password;
	}
	
	public ArrayList<OracleTablespace> getExcludedTablespaces() {
		return excludedTablespaces;
	}
	
	public Double getPercent() {
		return percent;
	}


	public void setPercent(Double percent) {
		this.percent = percent;
	}
	
	public boolean isTableSpaceExcluded(String searchTablespace) {
		for (OracleTablespace tablespace : excludedTablespaces) {
			if (tablespace.getName().equals(searchTablespace)) {
				return true;
			}
		}
		
		return false;
	}
	
}
