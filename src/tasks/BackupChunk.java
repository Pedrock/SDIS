package tasks;

import main.DBS;
import messages.Chunk;

public class BackupChunk implements Runnable{
	
	private static final int INITIAL_SLEEP = 1000;
	private static final int MAX_TRIES = 5;
	
	private Chunk chunk;
	
	public BackupChunk(Chunk chunk) {
		this.chunk = chunk;
	}

	@Override
	public void run() {
		int sleep = INITIAL_SLEEP;
		
		DBS.getMcListener().listenToStored(chunk);
		boolean success = false;
		for (int i = 0; i < MAX_TRIES && !success; i++)
		{
			DBS.getMessageBuilder().sendPutChunk(chunk);
			try {
				Thread.sleep(sleep);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			sleep *= 2;
			success = (DBS.getMcListener().getStoredCount(chunk) >= chunk.getReplicationDegree());
		}
		if (!success)
		{
			System.out.println("Replication degree not achieved");
		}
		else
		{
			System.out.println("Chunk backed up succesfully");
		}
	}
}
