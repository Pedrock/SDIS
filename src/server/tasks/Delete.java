package server.tasks;

import java.rmi.activation.UnknownObjectException;
import java.util.Set;

import server.main.DBS;

public class Delete implements Runnable {
	
	private String fileId;
	
	public Delete(String filename) throws UnknownObjectException {
		Set<String> ids = DBS.getDatabase().getSentFileIds(filename);
		if (ids == null || ids.isEmpty())
		{
			throw new UnknownObjectException(filename);
		}
		this.fileId = ids.iterator().next();
	}
	
	
	public void run() {
		DBS.getMessageBuilder().sendDelete(fileId);
	}
}