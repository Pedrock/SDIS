package server.tasks;

import server.main.DBS;
import server.messages.Chunk;

public class BackupChunk implements Runnable{
	
	private static final int INITIAL_SLEEP = 1000;
	private static final int MAX_TRIES = 5;
	
	private Chunk chunk;
	
	private boolean success = false;
	
	public BackupChunk(Chunk chunk) {
		this.chunk = chunk;
	}
	
	boolean wasSuccessful()
	{
		return success;
	}

	@Override
	public void run() {
		int sleep = INITIAL_SLEEP;
		DBS.getMcListener().notifyOnStored(this, chunk);
		boolean success = false;
		for (int i = 0; i < MAX_TRIES && !success; i++)
		{
			DBS.getMessageBuilder().sendPutChunk(chunk);
			synchronized(this) {
				try {
					wait(sleep);
				} catch (InterruptedException e) {}
			}
			sleep *= 2;
			success = (DBS.getMcListener().getStoredCount(chunk) >= chunk.getReplicationDegree());
		}
		DBS.getMcListener().stopListenToStored(chunk);
		if (!success)
		{
			System.out.println("Replication degree not achieved");
		}
		else
		{
			System.out.println("Chunk backed up succesfully");
			this.success = true;
		}
	}
}
