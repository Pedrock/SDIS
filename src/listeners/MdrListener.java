package listeners;

import java.io.IOException;
import java.util.HashMap;

import messages.Chunk;
import messages.ChunkID;

public class MdrListener extends Listener {
	
	private volatile HashMap<ChunkID,Runnable> runnables = new HashMap<ChunkID,Runnable >();
	private volatile HashMap<ChunkID,Chunk> chunks = new HashMap<ChunkID,Chunk >();
	
	public MdrListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public synchronized void handleChunk(int sender, String fileId, int chunkNumber, byte[] body) {
		ChunkID chunkId = new ChunkID(fileId, chunkNumber);
		Runnable runnable = runnables.get(chunkId);
		if (runnable != null)
		{
			Chunk chunk = new Chunk(fileId,chunkNumber,body,1);
			chunks.put(chunkId, chunk);
			synchronized (runnable) {
				runnable.notifyAll();
			}
		}
	}

	public synchronized void notifyOnChunk(Runnable runnable, ChunkID chunkID) {
		runnables.put(chunkID, runnable);
	}
	
	public synchronized Chunk getChunk(ChunkID chunkID) {
		runnables.remove(chunkID);
		return chunks.remove(chunkID);
	}

}
