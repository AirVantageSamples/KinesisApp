package net.airvantage;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

public class IRecordProcessorFactoryImplt implements IRecordProcessorFactory {
    
	private DataHandler handler;
	
	public IRecordProcessorFactoryImplt(DataHandler handler) {
		super();
		this.handler = handler;
	}
	
	
	public IRecordProcessor createProcessor() {
		
		return new RecordProcessor(handler);
	}

}
