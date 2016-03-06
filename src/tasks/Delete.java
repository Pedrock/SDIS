package tasks;

import main.DBS;

public class Delete implements Runnable {
	
	private String fileId;
	
	public Delete(String fileId) {
		this.fileId = fileId;
	}
	
	
	public void run() {
		DBS.getMessageBuilder().sendDelete(fileId);
	}
}