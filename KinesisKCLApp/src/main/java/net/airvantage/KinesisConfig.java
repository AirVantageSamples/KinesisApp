package net.airvantage;

import java.io.FileNotFoundException;
import java.util.Properties;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

public class KinesisConfig {
    
	private String appName;
	private int maxRecords;
	private String streamName;
	private String endPoint;
	private String workerId;
	private ProfileCredentialsProvider credentials;
		
	public KinesisConfig(String workerId) {
	 this.loadProperties();
	 this.workerId=workerId;
	 this.credentials = new ProfileCredentialsProvider();
	}

	public KinesisClientLibConfiguration createKinesiLibConfig() {
	  return new KinesisClientLibConfiguration(appName, streamName, credentials, workerId).withKinesisEndpoint(endPoint).withMaxRecords(maxRecords).withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
	}
	
	private  boolean loadProperties() {
		boolean res = false; 
		boolean error = true;
		Properties prop = null;
		// loading properties
		try {
            prop = PropertyLoader.load("config.properties");
            error = false;
		}catch (Exception e0) {    
		    try {
			  prop = PropertyLoader.load("src/main/java/net/airvantage/resources/config.properties");
			  error = false;
		    } catch (FileNotFoundException e) {
			      System.out.println("Could not find config.properties in the developement directory" + e.getMessage());
		    } catch (Exception e) {
			      System.out.println("Could not find any config.properties." + e.getMessage());
		    }
        }
		if (!error){
			this.appName = prop.getProperty("kinesis.AppName");
			this.maxRecords = Integer.parseInt(prop.getProperty("kinesis.maxRecords"));
			this.streamName = prop.getProperty("kinesis.streamName");
			this.endPoint = prop.getProperty("kinesis.endPoint");
			res=true;
			System.out.println("Configuration file for KinesisConfiguration has been successfully loaded.");
		}
		return res;
	}

}
