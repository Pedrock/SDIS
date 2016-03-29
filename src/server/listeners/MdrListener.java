package server.listeners;

import java.io.IOException;
import java.util.HashMap;

import server.messages.Chunk;
import server.messages.ChunkID;

public class MdrListener extends Listener {
	
	private HashMap<ChunkID,Runnable> runnables = new HashMap<ChunkID,Runnable >();
	private HashMap<ChunkID,Chunk> chunks = new HashMap<ChunkID,Chunk >();
	
	public MdrListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public void handleChunk(int sender, String fileId, int chunkNumber, byte[] body) {
		ChunkID chunkId = new ChunkID(fileId, chunkNumber);
		synchronized (runnables) {
			Runnable runnable = runnables.get(chunkId);
			if (runnable != null)
			{
				Chunk chunk = new Chunk(fileId,chunkNumber,body,1);
				synchronized (chunks) {
					chunks.put(chunkId, chunk);
					synchronized (runnable) {
						runnable.notifyAll();
					}
				}
			}
		}
	}

	public void notifyOnChunk(Runnable runnable, ChunkID chunkID) {
		synchronized (runnables) {
			runnables.put(chunkID, runnable);
		}
	}
	
	public Chunk getChunk(ChunkID chunkID) {
		synchronized (runnables) {
			synchronized (chunks) {
				runnables.remove(chunkID);
				return chunks.remove(chunkID);
			}
		}
	}

}
