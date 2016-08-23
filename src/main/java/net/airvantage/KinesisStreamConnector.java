package net.airvantage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

/**
 * This class creates the KCL Worker, configures and launches it
 *
 */

public class KinesisStreamConnector {

	private static final Logger logger = Logger.getLogger(DataHandler.class);

	// The worker Id
	private String workerId;
	// An instance of the KCL Worker
	private Worker worker;
	// IRecordProcessorFactory Object to instantiate the Record Processor
	private IRecordProcessorFactory factory;
	// An instance of KinesisClientLibConfiguration
	private KinesisClientLibConfiguration kinesisLibConfig;

	/**
	 * this constructor creates an instance of the record processor factory, an
	 * instance of kinesis configuration and instantiates the worker
	 * 
	 * @param handler
	 */

	public KinesisStreamConnector(DataHandler handler) {

		// initializing worker id
		try {
			this.workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		} catch (UnknownHostException e) {

			logger.error(e);
		}
		// Creating an instance of IRecordProcessorFactoryImplt
		this.factory = new IRecordProcessorFactoryImplt(handler);
		// creating an instance of KinesisConfig
		this.kinesisLibConfig = new KinesisConfig(workerId).createKinesiLibConfig();
		// creating the KCL Worker and configuring it
		this.worker = new Worker.Builder().recordProcessorFactory(factory).config(kinesisLibConfig).build();
	}

	/**
	 * This method enables to start the KCL Worker
	 */
	public void start() {
		this.worker.run();
	}

}
