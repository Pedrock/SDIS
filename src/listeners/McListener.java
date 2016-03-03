package listeners;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import messages.Chunk;
import messages.ChunkID;

public class McListener extends Listener {

	HashMap<ChunkID,HashSet<Integer>> storeCounter = new HashMap<ChunkID,HashSet<Integer> >();
	
	public McListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public synchronized void listenToStored(Chunk chunk)
	{
		storeCounter.put(chunk.getID(), new HashSet<Integer>());
	}
	
	public synchronized int getStoredCount(Chunk chunk)
	{
		return storeCounter.get(chunk.getID()).size();
	}
	
	public synchronized void stopListenToStored(Chunk chunk)
	{
		storeCounter.remove(chunk.getID());
	}

	public synchronized void handleStored(int sender, String fileId, int chunkNumber) {
		if (storeCounter.isEmpty()) return;
		ChunkID chunkId = new ChunkID(fileId, chunkNumber);
		HashSet<Integer> set = storeCounter.get(chunkId);
		if (set != null) storeCounter.get(chunkId).add(sender);
	}
}
