package server.listeners;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import server.messages.ChunkID;

public class MdbListener extends Listener {

	private HashMap<ChunkID,Runnable> runnables = new HashMap<ChunkID,Runnable >();
	private HashSet<ChunkID> receivedPutChunks = new HashSet<ChunkID>();
	
	public MdbListener(String address, int port) throws IOException {
		super(address, port);
	}
	
	public void handlePutChunk(int sender, String fileId, int chunkNumber, byte[] body) {
		ChunkID chunkId = new ChunkID(fileId, chunkNumber);
		synchronized (runnables) {
			Runnable runnable = runnables.get(chunkId);
			if (runnable != null)
			{
				synchronized (receivedPutChunks) {
					receivedPutChunks.add(chunkId);
					synchronized (runnable) {
						runnable.notifyAll();
					}
				}
			}
		}
	}
	
	public void notifyOnPutChunk(Runnable runnable, ChunkID chunkID) {
		synchronized (runnables) {
			runnables.put(chunkID, runnable);
		}
	}
	
	public boolean putChunkReceived(ChunkID chunkID) {
		synchronized (runnables) {
			synchronized (receivedPutChunks) {
				runnables.remove(chunkID);
				return receivedPutChunks.remove(chunkID);
			}
		}
	}
}
