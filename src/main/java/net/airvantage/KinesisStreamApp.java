package net.airvantage;

/**
 * 
 * This class is the main class it creates an instance of the dataHandler and
 * the KinesisStreamConnector and it connects the application to the kinesis
 * stream
 */

public class KinesisStreamApp {

	/**
	 * This main method launches the connection with the kinesis stream in order
	 * to retrieve data from the kinesis stream
	 * 
	 */

	public static void main(String[] args) {
		DataHandler handler = new DataHandler();
		KinesisStreamConnector kinesisStreamConnector = new KinesisStreamConnector(handler);
		kinesisStreamConnector.start();
	}
}
