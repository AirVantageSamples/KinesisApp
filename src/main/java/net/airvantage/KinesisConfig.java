package net.airvantage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;

/**
 * 
 * This class loads properties from the properties file to enable Kinesis Client
 * Configuration
 *
 */

public class KinesisConfig {

	private static final Logger logger = Logger.getLogger(KinesisConfig.class);

	// Application name
	private String appName;
	// Maximum of records received by call
	private int maxRecords;
	// Stream name
	private String streamName;
	// End point
	private String region;
	// Kinesis worker Id
	private String workerId;
	// Credentials Provider
	private ProfileCredentialsProvider credentials;

	/**
	 * This constructor loads configuration properties from the properties file
	 * and instantiates the credentials
	 * 
	 * @param workerId
	 */

	public KinesisConfig(String workerId) {
		this.loadProperties();
		this.workerId = workerId;
		this.credentials = new ProfileCredentialsProvider();
	}

	/**
	 * this class returns an instance of KinesisClientLibConfiguration
	 * 
	 * @return KinesisClientLibConfiguration
	 */

	public KinesisClientLibConfiguration createKinesiLibConfig() {
		return new KinesisClientLibConfiguration(appName, streamName, credentials, workerId)
				.withRegionName(region).withMaxRecords(maxRecords)
				.withInitialPositionInStream(InitialPositionInStream.TRIM_HORIZON);
	}

	/**
	 * This method loads the configuration properties from the properties file
	 * 
	 * @return boolean
	 */

	private boolean loadProperties() {
		boolean res = false;
		boolean error = true;
		Properties prop = null;
		// loading properties
		try {
			prop = PropertyLoader.load("config.properties");
			error = false;
		} catch (Exception e0) {

			logger.error(e0);

			try {
				prop = PropertyLoader.load("src/main/resources/config.properties");
				error = false;
			} catch (FileNotFoundException e) {
				logger.error("Could not find config.properties in the developement directory", e);
			} catch (IOException e) {
				logger.error("I/O Exception", e);
			}
		}
		if (!error) {
			this.appName = prop.getProperty("kinesis.AppName");
			this.maxRecords = Integer.parseInt(prop.getProperty("kinesis.maxRecords"));
			this.streamName = prop.getProperty("kinesis.streamName");
			this.region = prop.getProperty("kinesis.region");
			res = true;
			logger.info("Configuration file for KinesisConfiguration has been successfully loaded.");
		}
		return res;
	}

}
