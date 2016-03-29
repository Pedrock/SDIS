package server.tasks;

import java.rmi.activation.UnknownObjectException;

import server.main.DBS;
import server.messages.ChunkID;

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
	
	public Delete(ChunkID chunk) {
		this.fileId = chunk.getFileId();
	}
	
	public void runWithExceptions()
	{
		DBS.getMessageBuilder().sendDelete(fileId);
	}
	
	@Override
	public void run() {
		try
		{
			runWithExceptions();
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}