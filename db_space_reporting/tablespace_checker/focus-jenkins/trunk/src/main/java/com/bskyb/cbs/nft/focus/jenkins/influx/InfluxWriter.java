package com.bskyb.cbs.nft.focus.jenkins.influx;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApi;
import com.influxdb.client.domain.HealthCheck;
import com.influxdb.client.domain.HealthCheck.StatusEnum;
import com.influxdb.client.domain.WritePrecision;

public class InfluxWriter 
{
    private static Logger logger = LoggerFactory.getLogger(InfluxWriter.class);

	private static InfluxDBClient client = null;
	private static String bucket = null;
	private static String org = null;
	private static String url = null;
	private static WriteApi writeApi = null;

	public static boolean initialiseClient(String url, String org, String bucket, String token) 
	{
		InfluxWriter.bucket = bucket;
		InfluxWriter.org = org;
		InfluxWriter.url = url;
		boolean ok = true;
		client = InfluxDBClientFactory.create(url, token.toCharArray());
		HealthCheck health = client.health();
		
		if (health.getStatus().equals(StatusEnum.FAIL))
			ok = false;
		
		try 
		{
		  writeApi = client.getWriteApi();
		}
		catch (Exception e)
		{
			logger.error("Failed to create write API DB {}, Organisation {}, Bucket {} : {}", url, org, bucket);
			ok = false;
		}
		
		return ok;
	}
	
	public static void shutdownClient()
	{
		writeApi.close();
		client.close();
	}

	public static void writeData(String data) 
	{
		try 
		{ 
			writeApi.writeRecord(bucket, org, WritePrecision.NS, data);
		}
		catch (Exception e)
		{
			logger.error("Failed to write metric to DB {}, Organisation {}, Bucket {} : {}", url, org, bucket, data );
		}
	}

}