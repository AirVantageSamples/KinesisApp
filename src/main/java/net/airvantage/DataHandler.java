package net.airvantage;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.services.kinesis.model.Record;

/**
 * This class handles the received records, it displays the messages but it is
 * possible to link this application to a database for example.
 *
 */

public class DataHandler {

	private static final Logger logger = Logger.getLogger(DataHandler.class);

	/**
	 * This method process the received records.
	 * 
	 * @param records
	 */

	public void processMyRecords(List<Record> records) {

		// Display received Records on the console and handles all the
		// exceptions while processing
		// records
		for (Record rc : records) {
			try {
				String message = new String(rc.getData().array(), "UTF-8");
				logger.debug(message);

			} catch (UnsupportedEncodingException e) {

				logger.error(e);
			}
		}
	}

}
