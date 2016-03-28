package server.tasks;

import java.rmi.activation.UnknownObjectException;

import server.main.DBS;

public class Delete implements Runnable {
	
	private String fileId;
	
	public Delete(String filename) throws UnknownObjectException {
		String id = DBS.getDatabase().getLastSentFileId(filename);
		if (id == null)
		{
			throw new UnknownObjectException(filename);
		}
		this.fileId = id;
	}
	
	
	public void run() {
		DBS.getMessageBuilder().sendDelete(fileId);
	}
}