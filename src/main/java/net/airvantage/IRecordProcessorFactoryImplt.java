package net.airvantage;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessorFactory;

/**
 * 
 * This class implements the IRecordProcessorFactory interface
 *
 */

public class IRecordProcessorFactoryImplt implements IRecordProcessorFactory {

	// The dataHandler will handles the received records
	private DataHandler handler;

	public IRecordProcessorFactoryImplt(DataHandler handler) {
		this.handler = handler;
	}

	/**
	 * this method creates a new instance of RecordProcessor
	 */
	@Override
	public IRecordProcessor createProcessor() {

		return new RecordProcessor(handler);
	}

}
