package server.tasks;

import java.rmi.activation.UnknownObjectException;

import server.main.DBS;
import server.main.PeerError;
import server.messages.ChunkID;

public class Delete implements Runnable {
	
	private static final int INITIAL_SLEEP = 500;
	private static final int MAX_TRIES = 5;
	
	private String filename;
	private String fileId;
	
	public Delete(String filename) throws UnknownObjectException {
		String id = DBS.getDatabase().getLastSentFileId(filename);
		if (id == null)
		{
			throw new UnknownObjectException(filename);
		}
		this.filename = filename;
		this.fileId = id;
	}
	
	public Delete(ChunkID chunk) {
		this.filename = null;
		this.fileId = chunk.getFileId();
	}
	
	public void runWithExceptions() throws Exception
	{
		if (filename != null)
			DBS.getDatabase().deleteMyFile(filename, fileId);
		int sleep = INITIAL_SLEEP;
		for (int i = 0; i < MAX_TRIES; i++)
		{
			DBS.getMessageBuilder().sendDelete(fileId);
			if (!DBS.isRunning()) throw new PeerError("Server stopped");
			sleep *= 2;
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {}
			if (!DBS.isRunning()) throw new PeerError("Server stopped");
		}
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