package net.airvantage;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;

/**
 * This class implementing the KCL class IRecordProcessor will instantiates a
 * Record Processor that process data fetched from Amazon kinesis stream
 *
 */

public class RecordProcessor implements IRecordProcessor {

	private static final Logger logger = Logger.getLogger(DataHandler.class);

	private String shardId;
	private DataHandler dataHandler;

	/**
	 * This Record Processor constructor will initialize the data Handle that
	 * will handles the received records
	 * 
	 * @param handler
	 */
	public RecordProcessor(DataHandler handler) {
		this.dataHandler = handler;
	}

	/**
	 * initializing the Record Processor
	 */

	@Override
	public void initialize(InitializationInput shardId) {
		logger.info("initialization of record processor for shard:" + shardId);
		this.shardId = shardId.getShardId();
	}

	/**
	 * this method process the records by calling the DataHandler methods and
	 * make the checkpoint after processing data
	 */

	@Override
	public void processRecords(ProcessRecordsInput processRecordsInput) {
		// process received Records (display records data, saving records, ...)
		logger.info("processing records from shard:" + shardId);
		this.dataHandler.processMyRecords(processRecordsInput.getRecords());
		// checkPoint
		checkPointRecords(processRecordsInput.getCheckpointer());
	}

	/**
	 * When the shut down reason is TERMINATE this method will make the
	 * checkpoint
	 */

	@Override
	public void shutdown(ShutdownInput shutdownInput) {

		if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
			checkPointRecords(shutdownInput.getCheckpointer());
		}
	}

	/**
	 * This method make the checkpoint and handles checkpoint's exceptions
	 * 
	 * @param checkpointer
	 */
	private void checkPointRecords(IRecordProcessorCheckpointer checkpointer) {
		try {
			checkpointer.checkpoint();
		} catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException
				| ShutdownException e) {
			logger.error(e);
		}
	}

}