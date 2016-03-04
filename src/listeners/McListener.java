package listeners;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import messages.Chunk;
import messages.ChunkID;

public class McListener extends Listener {
	
	private class StoredMapValue {
		Runnable runnable;
		HashSet<Integer> senders;
		int replication;
		
		StoredMapValue(Runnable runnable, int replication) {
			super();
			this.runnable = runnable;
			this.senders = new HashSet<Integer>();
			this.replication = replication;
		}
	}

	private volatile HashMap<ChunkID,StoredMapValue> storedMap = new HashMap<ChunkID,StoredMapValue >();
	
	
	public McListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public synchronized void notifyOnStored(Runnable runnable, Chunk chunk) 
	{
		storedMap.put(chunk.getID(), new StoredMapValue(runnable,chunk.getReplicationDegree()));
	}
	
	public synchronized int getStoredCount(Chunk chunk)
	{
		return storedMap.get(chunk.getID()).senders.size();
	}
	
	public synchronized void stopListenToStored(Chunk chunk)
	{
		storedMap.remove(chunk.getID());
	}

	public synchronized void handleStored(int sender, String fileId, int chunkNumber) {
		if (storedMap.isEmpty()) return;
		ChunkID chunkId = new ChunkID(fileId, chunkNumber);
		StoredMapValue info = storedMap.get(chunkId);
		if (info != null)
		{
			info.senders.add(sender);
			if (info.senders.size() >= info.replication) 
			{
				synchronized (info.runnable) {
					info.runnable.notifyAll();
				}
			}
		}
	}
}
