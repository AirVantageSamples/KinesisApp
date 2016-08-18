package net.airvantage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;

public class KinesisStreamConnector {

	 private String workerId;
	 private Worker worker;
	 private IRecordProcessorFactory factory;
	 private KinesisClientLibConfiguration kinesisLibConfig; 
	 
	 
	public KinesisStreamConnector(DataHandler handler){
		
		// initializing worker id
		try {
			this.workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		this.factory = new IRecordProcessorFactoryImplt(handler);
		this.kinesisLibConfig = new KinesisConfig(workerId).createKinesiLibConfig();
		this.worker = new Worker.Builder().recordProcessorFactory(factory).config(kinesisLibConfig).build();
	}


	public  void start() {
	    this.worker.run();
	}

}
