package net.airvantage;

public class KinesisStreamApp {
    
	private KinesisStreamConnector kinesisStreamConnector;
	
	public void main(String[] args) {
		DataHandler handler =  new DataHandler();
		this.kinesisStreamConnector = new KinesisStreamConnector(handler);
		this.kinesisStreamConnector.start();
	}
}
