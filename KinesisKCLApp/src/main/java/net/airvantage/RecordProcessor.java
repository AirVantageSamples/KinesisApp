package net.airvantage;

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

public class RecordProcessor implements IRecordProcessor {

	private String shardId;
	private DataHandler dataHandler;

	public RecordProcessor(DataHandler handler) {
		this.dataHandler = handler;
	}

	public void initialize(InitializationInput _shardId) {
		System.out.println("initialization of record processor for shard:" + _shardId);
		this.shardId = _shardId.getShardId();
	}

	public void processRecords(ProcessRecordsInput processRecordsInput) {
		// process received Records (display records data, saving records, ...)
		System.out.println("processing records from shard:" + shardId);
		this.dataHandler.processMyRecords(processRecordsInput.getRecords());
		// checkPoint
		checkPointRecords(processRecordsInput.getCheckpointer());
	}

	public void shutdown(ShutdownInput shutdownInput) {

		if (shutdownInput.getShutdownReason() == ShutdownReason.TERMINATE) {
			checkPointRecords(shutdownInput.getCheckpointer());
		}
	}

	// checkpoint and handle checkpoint's exceptions
	private void checkPointRecords(IRecordProcessorCheckpointer checkpointer) {
		try {
			checkpointer.checkpoint();
		} catch (KinesisClientLibDependencyException e) {
			System.out.println(e.getMessage());
		} catch (InvalidStateException e) {
			System.out.println(e.getMessage());
		} catch (ThrottlingException e) {
			System.out.println(e.getMessage());
		} catch (ShutdownException e) {
			System.out.println(e.getMessage());
		}
	}

}