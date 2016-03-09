package listeners;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import filesystem.Database;
import main.DBS;
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

	private HashMap<ChunkID,StoredMapValue> storedMap = new HashMap<ChunkID,StoredMapValue >();
	
	public McListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public synchronized void notifyOnStored(Runnable runnable, Chunk chunk) 
	{
		storedMap.put(chunk.getID(), new StoredMapValue(runnable,chunk.getReplicationDegree()));
	}
	
	public synchronized Integer getStoredCount(Chunk chunk)
	{
		return storedMap.get(chunk.getID()).senders.size();
	}
	
	public synchronized void stopListenToStored(Chunk chunk)
	{
		storedMap.remove(chunk.getID());
	}

	public synchronized void handleStored(int sender, String fileId, int chunkNumber) {
		ChunkID chunkID = new ChunkID(fileId, chunkNumber);
		DBS.getDatabase().addChunkPeer(chunkID, sender);
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

	public synchronized void handleDelete(int sender, String fileId) {
		Database db = DBS.getDatabase();
		Set<Integer> chunksSet = db.getFileChunks(fileId);
		Integer[] chunks = chunksSet.toArray(new Integer[chunksSet.size()]);
		if (chunks != null)
		{
			for (Integer chunk : chunks) 
			{
				ChunkID chunkId = new ChunkID(fileId, chunk);
				File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
				file.delete();
				db.removeReceivedBackup(chunkId);
			}
		}
	}

}
