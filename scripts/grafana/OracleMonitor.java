package com.bskyb.cbs.nft.monitoring.oracle;

import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.yaml.snakeyaml.Yaml;

public class OracleMonitor {
  private static final String CONFIG_ARG = "config.yml";
  
  public static final String DUMP_FOR_PRINTING = "dumpForPrinting";
  
  private Multimap<String, String> valueMap;
  
  private ArrayList<SessionPoint> sessionData;
  
  private Configuration config;
  
  private InfluxDB influxDB;
  
  private BatchPoints batchPoints;
  
  public static void main(String[] args) throws Exception {
    OracleMonitor om = new OracleMonitor();
    if (args.length == 0) {
      om.execute("config.yml");
    } else {
      om.execute(args[0]);
    } 
  }
  
  public OracleMonitor() throws ClassNotFoundException {
    Class.forName("oracle.jdbc.driver.OracleDriver");
  }
  
  public void execute(String configFile) throws MetricsGatheringException {
    try {
      Configuration config = readFromYMLFile(configFile);
      fetchMetrics(config, Queries.queries);
      persistMetrics(config);
    } catch (Exception e) {
      throw new MetricsGatheringException("Oracle DB Monitoring Task completed with failures.", e);
    } 
  }
  
  private Configuration readFromYMLFile(String configFile) throws MetricsGatheringException {
    ClassLoader classLoader = getClass().getClassLoader();
    Yaml yaml = new Yaml();
    try {
      File file = new File(classLoader.getResource(configFile).getFile());
      FileReader fr = new FileReader(file);
      BufferedReader reader = new BufferedReader(fr);
      this.config = (Configuration)yaml.loadAs(reader, Configuration.class);
    } catch (IOException e) {
      throw new MetricsGatheringException("Unable to load config file");
    } 
    return this.config;
  }
  
  private void fetchMetrics(Configuration config, String[] queries) throws Exception {
    this.valueMap = (Multimap<String, String>)ArrayListMultimap.create();
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = connect(config);
      stmt = conn.createStatement();
      byte b;
      int i;
      String[] arrayOfString;
      for (i = (arrayOfString = queries).length, b = 0; b < i; ) {
        String query = arrayOfString[b];
        ResultSet rs = null;
        try {
          rs = stmt.executeQuery(query);
          if (query.equalsIgnoreCase("SELECT a.sample_time, CASE WHEN a.session_state = 'ON CPU' THEN 'CPU + CPU Wait' ELSE NVL ( a.wait_class , '(null)' ) END AS wait_class, CASE WHEN a.module IS NULL THEN NVL ( LOWER ( u.username ) , '(null)' ) WHEN a.module IN ( 'JDBC Thin Client' , 'perl.exe' , 'SQL*Plus' ) THEN LOWER ( u.username ) WHEN a.module LIKE 'sqlplus%' THEN LOWER ( u.username ) WHEN a.module LIKE 'oracle@%' THEN LOWER ( u.username ) ELSE a.module END AS component, COUNT(*) AS active_sessions  FROM v$active_session_history a LEFT OUTER JOIN dba_users u ON u.user_id = a.user_id WHERE a.sample_time >= SYSTIMESTAMP - 1/24/60 GROUP BY a.sample_time, CASE WHEN a.session_state = 'ON CPU' THEN 'CPU + CPU Wait' ELSE NVL ( a.wait_class , '(null)' ) END , CASE WHEN a.module IS NULL THEN NVL ( LOWER ( u.username ) , '(null)' ) WHEN a.module IN ( 'JDBC Thin Client' , 'perl.exe' , 'SQL*Plus' ) THEN LOWER ( u.username ) WHEN a.module LIKE 'sqlplus%' THEN LOWER ( u.username ) WHEN a.module LIKE 'oracle@%' THEN LOWER ( u.username ) ELSE a.module END ORDER BY 1 , 2 , 3")) {
            this.sessionData = new ArrayList<>();
            while (rs.next())
              this.sessionData.add(new SessionPoint(rs.getTimestamp(1), rs.getString(2), rs.getString(3), rs.getInt(4))); 
          } else {
            while (rs.next()) {
              String key = rs.getString(1);
              String value = rs.getString(2);
              if (!value.equalsIgnoreCase(""))
                this.valueMap.put(key.toUpperCase(), value); 
              if (query.equalsIgnoreCase("select df.tablespace_name, round(100 * ( (df.totalspace - tu.totalusedspace)/ df.totalspace)) PercentFree from (select tablespace_name, round(sum(bytes) / 1048576) totalSpace from dba_data_files group by tablespace_name) df, (select round(sum(bytes)/(1024*1024)) totalusedspace, tablespace_name from dba_segments group by tablespace_name) tu where df.tablespace_name = tu.tablespace_name"))
                this.valueMap.put(key.toUpperCase(), "dumpForPrinting"); 
            } 
          } 
        } catch (Exception ex) {
          throw ex;
        } finally {
          close(rs, null, null);
        } 
        b++;
      } 
    } finally {
      close(null, stmt, conn);
    } 
  }
  
  private Connection connect(Configuration config) throws SQLException {
    String host = config.getHost();
    String port = String.valueOf(config.getPort());
    String userName = config.getUsername();
    String password = config.getPassword();
    String sid = config.getSid();
    if (Strings.isNullOrEmpty(port))
      port = "1521"; 
    if (Strings.isNullOrEmpty(sid))
      sid = "orcl"; 
    String connStr = String.format("jdbc:oracle:thin:@%s:%s:%s", new Object[] { host, port, sid });
    Connection conn = DriverManager.getConnection(connStr, userName, password);
    return conn;
  }
  
  private void persistMetrics(Configuration config) {
    InfluxDB influxDB = InfluxDBFactory.connect("http://wnrep010:8086 ", "root", "root");
    influxDB.setLogLevel(InfluxDB.LogLevel.FULL);
    this.batchPoints = BatchPoints.database("oracle").tag("env", config.getEnvironment()).tag("db", config.getName())
      .tag("host", config.getHost()).tag("sid", config.getSid()).retentionPolicy("autogen").consistency(InfluxDB.ConsistencyLevel.ALL).build();
    Point.Builder pointBuilder = Point.measurement("ResourceUtilisation")
      .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    pointBuilder.addField("TotalSessions", Integer.parseInt(getString("Sessions")));
    pointBuilder.addField("SessionUtil", Integer.parseInt(getString("% of max sessions")));
    pointBuilder.addField("OpenCursorUtil", Integer.parseInt(getString("% of max open cursors")));
    pointBuilder.addField("FreeSharedPool", Integer.parseInt(getString("Shared Pool Free %")));
    pointBuilder.addField("TempSpaceUsed", Integer.parseInt(getString("Temp Space Used")));
    pointBuilder.addField("PGAAllocated", Integer.parseInt(getString("Total PGA Allocated")));
    pointBuilder.addField("HostCPU", Double.parseDouble(getString("Host CPU Utilization (%)")));
    this.batchPoints.point(pointBuilder.build());
    for (Iterator<SessionPoint> iterator = this.sessionData.iterator(); iterator.hasNext(); ) {
      pointBuilder = Point.measurement("SessionBreakdown");
      SessionPoint sessionPoint = iterator.next();
      pointBuilder.addField("count", sessionPoint.getSessionCount()).tag("component", sessionPoint.getComponent()).tag("waitClass", sessionPoint.getWaitClass()).time(sessionPoint.getTimestamp().longValue(), TimeUnit.MILLISECONDS);
      this.batchPoints.point(pointBuilder.build());
    } 
    pointBuilder = Point.measurement("Activity").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    pointBuilder.addField("OSLoad", Integer.parseInt(getString("Current OS Load")));
    pointBuilder.addField("ExecutionRate", Integer.parseInt(getString("Executions Per Sec")));
    pointBuilder.addField("ActiveSessions", Integer.parseInt(getString("Average Active Sessions")));
    pointBuilder.addField("ActiveSessionsPerCPU", Integer.parseInt(getString("Average Active Sessions per logical CPU")));
    this.batchPoints.point(pointBuilder.build());
    pointBuilder = Point.measurement("IO").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    pointBuilder.addField("MBperSec", Integer.parseInt(getString("I/O Megabytes per Second")));
    pointBuilder.addField("LogicalReadRate", Integer.parseInt(getString("Logical Reads Per Sec")));
    pointBuilder.addField("PhysicalReadRate", Integer.parseInt(getString("Physical Reads Per Sec")));
    pointBuilder.addField("PhysicalReadByteRate", Integer.parseInt(getString("Physical Read Total Bytes Per Sec")));
    pointBuilder.addField("PhysicalWriteByteRate", Integer.parseInt(getString("Physical Write Total Bytes Per Sec")));
    this.batchPoints.point(pointBuilder.build());
    pointBuilder = Point.measurement("WaitClass").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    pointBuilder.addField("Administrative", Integer.parseInt(getString("Wait Class Breakdown|Administrative")));
    pointBuilder.addField("Application", Integer.parseInt(getString("Wait Class Breakdown|Application")));
    pointBuilder.addField("Commit", Integer.parseInt(getString("Wait Class Breakdown|Commit")));
    pointBuilder.addField("Concurrency", Integer.parseInt(getString("Wait Class Breakdown|Concurrency")));
    pointBuilder.addField("Configuration", Integer.parseInt(getString("Wait Class Breakdown|Configuration")));
    pointBuilder.addField("CPU", Integer.parseInt(getString("Wait Class Breakdown|CPU")));
    pointBuilder.addField("Network", Integer.parseInt(getString("Wait Class Breakdown|Network")));
    pointBuilder.addField("Other", Integer.parseInt(getString("Wait Class Breakdown|Other")));
    pointBuilder.addField("Scheduler", Integer.parseInt(getString("Wait Class Breakdown|Scheduler")));
    pointBuilder.addField("System I/O", Integer.parseInt(getString("Wait Class Breakdown|System I/O")));
    pointBuilder.addField("User I/O", Integer.parseInt(getString("Wait Class Breakdown|User I/O")));
    this.batchPoints.point(pointBuilder.build());
    pointBuilder = Point.measurement("Efficiency").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    pointBuilder.addField("DbCpuTimeRatio", Integer.parseInt(getString("Database CPU Time Ratio")));
    pointBuilder.addField("DbWaitTimeRatio", Integer.parseInt(getString("Database Wait Time Ratio")));
    pointBuilder.addField("MemorySortsRatio", Integer.parseInt(getString("Memory Sorts Ratio")));
    pointBuilder.addField("ExecuteWithoutParseRatio", Integer.parseInt(getString("Execute Without Parse Ratio")));
    pointBuilder.addField("SoftParseRatio", Integer.parseInt(getString("Soft Parse Ratio")));
    pointBuilder.addField("ResponseTimePerTxn", (Integer.parseInt(getString("Response Time Per Txn")) / 10));
    pointBuilder.addField("SQLServiceResponseTime", (Integer.parseInt(getString("SQL Service Response Time")) / 10));
    this.batchPoints.point(pointBuilder.build());
    for (Map.Entry<String, Collection<String>> entry : (Iterable<Map.Entry<String, Collection<String>>>)this.valueMap.asMap().entrySet()) {
      if (((Collection)entry.getValue()).contains("dumpForPrinting")) {
        pointBuilder = Point.measurement("Tablespace").time(System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        pointBuilder.addField("free", Integer.parseInt(getString(entry.getKey()))).tag("name", entry.getKey());
        this.batchPoints.point(pointBuilder.build());
      } 
    } 
    List points = this.batchPoints.getPoints();
    for (Iterator<Point> iterator1 = points.iterator(); iterator1.hasNext(); ) {
      Point point = iterator1.next();
      System.out.println(point.lineProtocol());
    } 
  }
  
  private void printDBMetrics(Configuration config) {
    String metricPath = String.valueOf(config.getEnvironment()) + config.getSid() + "|";
    String resourceUtilizationMetricPath = String.valueOf(metricPath) + "Resource Utilization|";
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "Total Sessions", getString("Sessions"));
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "% of max sessions", getString("% of max sessions"));
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "% of max open cursors", getString("% of max open cursors"));
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "Shared Pool Free %", getString("Shared Pool Free %"));
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "Temp Space Used", getString("Temp Space Used"));
    printMetric(String.valueOf(resourceUtilizationMetricPath) + "Total PGA Allocated", getString("Total PGA Allocated"));
    String activityMetricPath = String.valueOf(metricPath) + "Activity|";
    printMetric(String.valueOf(activityMetricPath) + "Active Sessions Current", getString("Active User Sessions"));
    printMetric(String.valueOf(activityMetricPath) + "Average Active Sessions per logical CPU", getString("Average Active Sessions per logical CPU"));
    printMetric(String.valueOf(activityMetricPath) + "Average Active Sessions", getString("Average Active Sessions"));
    printMetric(String.valueOf(activityMetricPath) + "Current OS Load", getString("Current OS Load"));
    printMetric(String.valueOf(activityMetricPath) + "DB Block Changes Per Sec", getString("DB Block Changes Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "DB Block Changes Per Txn", getString("DB Block Changes Per Txn"));
    printMetric(String.valueOf(activityMetricPath) + "DB Block Gets Per Sec", getString("DB Block Gets Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "DB Block Gets Per Txn", getString("DB Block Gets Per Txn"));
    printMetric(String.valueOf(activityMetricPath) + "Executions Per Sec", getString("Executions Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "Executions Per Txn", getString("Executions Per Txn"));
    printMetric(String.valueOf(activityMetricPath) + "I/O|I/O Megabytes per Second", getString("I/O Megabytes per Second"));
    printMetric(String.valueOf(activityMetricPath) + "I/O|Logical Reads Per Sec", getString("Logical Reads Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "I/O|Physical Reads Per Sec", getString("Physical Reads Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "I/O|Physical Read Total Bytes Per Sec", getString("Physical Read Total Bytes Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "I/O|Physical Write Total Bytes Per Sec", getString("Physical Write Total Bytes Per Sec"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Administrative", getString("Wait Class Breakdown|Administrative"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Application", getString("Wait Class Breakdown|Application"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Commit", getString("Wait Class Breakdown|Commit"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Concurrency", getString("Wait Class Breakdown|Concurrency"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Configuration", getString("Wait Class Breakdown|Configuration"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|CPU", getString("Wait Class Breakdown|CPU"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Network", getString("Wait Class Breakdown|Network"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Other", getString("Wait Class Breakdown|Other"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|Scheduler", getString("Wait Class Breakdown|Scheduler"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|System I/O", getString("Wait Class Breakdown|System I/O"));
    printMetric(String.valueOf(activityMetricPath) + "Wait Class Breakdown|User I/O", getString("Wait Class Breakdown|User I/O"));
    String efficiencyMetricPath = String.valueOf(metricPath) + "Efficiency|";
    printMetric(String.valueOf(efficiencyMetricPath) + "Database CPU Time Ratio", getString("Database CPU Time Ratio"));
    printMetric(String.valueOf(efficiencyMetricPath) + "Database Wait Time Ratio", getString("Database Wait Time Ratio"));
    printMetric(String.valueOf(efficiencyMetricPath) + "Memory Sorts Ratio", getString("Memory Sorts Ratio"));
    printMetric(String.valueOf(efficiencyMetricPath) + "Execute Without Parse Ratio", getString("Execute Without Parse Ratio"));
    printMetric(String.valueOf(efficiencyMetricPath) + "Soft Parse Ratio", getString("Soft Parse Ratio"));
    printMetric(String.valueOf(efficiencyMetricPath) + "Response Time Per Txn", getString("Response Time Per Txn"));
    printMetric(String.valueOf(efficiencyMetricPath) + "SQL Service Response Time", getString("SQL Service Response Time"));
    String tableSpaceMetricPath = String.valueOf(metricPath) + "TableSpaceMetrics|";
    for (Map.Entry<String, Collection<String>> entry : (Iterable<Map.Entry<String, Collection<String>>>)this.valueMap.asMap().entrySet()) {
      if (((Collection)entry.getValue()).contains("dumpForPrinting"))
        printMetric(String.valueOf(tableSpaceMetricPath) + (String)entry.getKey() + "|Free %", getString(entry.getKey())); 
    } 
  }
  
  protected void printMetric(String metricName, String value) {
    if (!Strings.isNullOrEmpty(value));
  }
  
  protected void close(ResultSet rs, Statement stmt, Connection conn) {
    if (rs != null)
      try {
        rs.close();
      } catch (Exception exception) {} 
    if (stmt != null)
      try {
        stmt.close();
      } catch (Exception exception) {} 
    if (conn != null)
      try {
        conn.close();
      } catch (Exception exception) {} 
  }
  
  protected String getString(float num) {
    int result = Math.round(num);
    return Integer.toString(result);
  }
  
  protected String getString(String key) {
    return getString(key, true);
  }
  
  protected String getString(String key, boolean convertUpper) {
    if (convertUpper)
      key = key.toUpperCase(); 
    List<String> values = (List<String>)this.valueMap.get(key);
    if (values.size() < 1)
      return "0"; 
    String strResult = values.get(0);
    if (strResult == null)
      return "0"; 
    float result = Float.valueOf(strResult).floatValue();
    String resultStr = getString(result);
    return resultStr;
  }
}
 