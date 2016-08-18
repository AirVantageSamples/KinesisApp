package net.airvantage;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.services.kinesis.model.Record;

public class DataHandler {
     
	private ArrayList<Record> receivedRecords;
	
	public DataHandler(){
		receivedRecords = new ArrayList<Record>();
	}
	
	public void save(List<Record> records) {
		receivedRecords.addAll(records);		
	}
	
	public void processMyRecords(List<Record> records) {
		
		// Display Records and handling all the exceptions while processing records 
		for (Record rc : records){
			try {
				String message = new String(rc.getData().array(), "UTF-8");
				System.out.println(message);
				
			} catch (UnsupportedEncodingException e) {
			
				e.printStackTrace();
			}
		}
		// saving raw data 
		this.save(records);
	}


}
