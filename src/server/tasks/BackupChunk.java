package server.tasks;

import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import server.main.DBS;
import server.messages.Chunk;

public class BackupChunk implements Callable<Boolean>{
	
	private static final int INITIAL_SLEEP = 1000;
	private static final int MAX_TRIES = 5;
	private static final int MAX_THREADS = 100;
	
	private static final Semaphore semaphore = new Semaphore(MAX_THREADS, true);
	
	private Chunk chunk;
	
	private boolean success = false;
	private boolean nonInitiator = false;
	
	public BackupChunk(Chunk chunk)
	{
		try {
			semaphore.acquire();
		} catch (InterruptedException e) {}
		this.chunk = chunk;
	}
	
	public BackupChunk(Chunk chunk, boolean nonInitiator) {
		this(chunk);
		this.nonInitiator = nonInitiator;
	}
	
	boolean wasSuccessful()
	{
		return success;
	}

	@Override
	public Boolean call() throws Exception {
		try 
		{
			if (!DBS.isRunning()) return false;
			int sleep = INITIAL_SLEEP;
			DBS.getMcListener().notifyOnStored(this, chunk);
			success = false;
			for (int i = 0; i < MAX_TRIES && !success; i++)
			{
				DBS.getMessageBuilder().sendPutChunk(chunk);
				synchronized(this) {
					try {
						this.wait(sleep);
					} catch (InterruptedException e) {}
				}
				sleep *= 2;
				success = (DBS.getMcListener().getStoredCount(chunk) >= chunk.getReplicationDegree());
			
				if (nonInitiator && !DBS.getDatabase().hasBackup(chunk.getID()))
				{
					System.out.println("No backup");
					break;
				}
				if (!DBS.isRunning()) break;
			}
			DBS.getMcListener().stopListenToStored(chunk);
			if (!DBS.isRunning()) return false;
			if (!success)
				System.out.println("Replication degree not achieved");
			else
				System.out.println("Chunk backed up succesfully");
			return success;
		}
		finally
		{
			semaphore.release();
		}
	}
	
	public void run()
	{
		try
		{
			this.call();
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}
