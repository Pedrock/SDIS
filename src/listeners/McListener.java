package listeners;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import messages.Chunk;

public class McListener extends Listener {

	HashMap<Chunk,HashSet<Integer>> storeCounter = new HashMap<Chunk,HashSet<Integer> >();
	
	
	public McListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public void listenToStored(Chunk chunk)
	{
		storeCounter.put(chunk, new HashSet<Integer>());
	}
	
	public int getStoredCount(Chunk chunk)
	{
		return storeCounter.get(chunk).size();
	}
	
	public void stopListenToStored(Chunk chunk)
	{
		storeCounter.remove(chunk);
	}

	public void handleStored(int sender, String fileId, int chunkNumber) {
		Chunk chunkId = new Chunk(fileId, chunkNumber);
		storeCounter.get(chunkId).add(sender);
	}
}
