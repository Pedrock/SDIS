package server.listeners;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import server.main.DBS;
import server.messages.Chunk;
import server.messages.ChunkID;

public class McListener extends Listener {
	
	private class StoredMapValue {
		Object runnable;
		HashSet<Integer> senders;
		int replication;
		
		StoredMapValue(Object runnable, int replication) {
			super();
			this.runnable = runnable;
			this.senders = new HashSet<Integer>();
			this.replication = replication;
		}
	}

	private HashMap<ChunkID,StoredMapValue> storedMap = new HashMap<ChunkID,StoredMapValue >();
	
	public McListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public void notifyOnStored(Object runnable, Chunk chunk) 
	{
		synchronized (storedMap) {
			storedMap.put(chunk.getID(), new StoredMapValue(runnable,chunk.getReplicationDegree()));
		}
	}
	
	public Integer getStoredCount(Chunk chunk)
	{
		synchronized (storedMap) {
			StoredMapValue v = storedMap.get(chunk.getID());
			if (v == null) return 0;
			return v.senders.size();
		}
	}
	
	public void stopListenToStored(Chunk chunk)
	{
		synchronized (storedMap) {
			storedMap.remove(chunk.getID());
		}
	}

	public void handleStored(int sender, String fileId, int chunkNumber) {
		ChunkID chunkID = new ChunkID(fileId, chunkNumber);
		DBS.getDatabase().addChunkPeer(chunkID, sender);
		synchronized (storedMap) {
			if (storedMap.isEmpty()) return;
			StoredMapValue info = storedMap.get(chunkID);
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

}
