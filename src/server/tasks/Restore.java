package server.tasks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.activation.UnknownObjectException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import server.filesystem.FileManager;
import server.main.DBS;
import server.main.PeerError;
import server.messages.Chunk;
import server.messages.ChunkID;

public class Restore implements Runnable{
	
	private static class ChunkRestore implements Callable<Chunk>
	{
		private String fileId;
		private int chunkN;
		
		ChunkRestore(String fileId, int chunkN) {
			this.fileId = fileId;
			this.chunkN = chunkN;
		}
		
		@Override
		public Chunk call() throws Exception {
			int sleep = INITIAL_SLEEP;
			int n_try = 0;
			boolean chunk_received = false;
			
			ChunkID chunkID = new ChunkID(fileId, chunkN);
			
			try
			{
				DBS.getMdrListener().notifyOnChunk(this, chunkID);
				
				for (; n_try < MAX_TRIES && !chunk_received; n_try++)
				{
					DBS.getMessageBuilder().sendGetChunk(fileId, chunkN);
					synchronized(this) {
						try {
							this.wait(sleep);
						} catch (InterruptedException e) {}
					}
					sleep *= 2;
					Chunk chunk = DBS.getMdrListener().getChunk(chunkID);
					if (chunk != null)
					{
						System.out.println("Valid chunk received");
						return chunk;
					}
					if (!DBS.isRunning()) throw new PeerError("Server stopped");
				}
				if (!DBS.isRunning()) throw new PeerError("Server stopped");
				return null;
			}
			finally
			{
				DBS.getMdrListener().stopListenToChunk(chunkID);
			}
		}
		
	}
	
	private static final int INITIAL_SLEEP = 1000;
	private static final int MAX_TRIES = 5;
	private static final int MAX_THREADS = 100;
	
	private String filename;
	private String fileId;
	
	public Restore(String filename) throws UnknownObjectException {
		this.filename = filename;
		String id = DBS.getDatabase().getLastSentFileId(filename);
		if (id == null)
		{
			throw new UnknownObjectException(filename);
		}
		this.fileId = id;
	}
	
	public Restore(String filename, String filehash) {
		this.filename = filename;
		this.fileId = filehash;
	}
	
	@Override
	public void run() {
		try
		{
			runWithExceptions();
		}
		catch(Exception ex) {
			System.out.println(ex.getMessage());
		}
	}
	
	public void runWithExceptions() throws Exception {
		FileManager fm = DBS.getRestoredFileManager();
		File file = fm.getFile(filename);
		if (file.exists())
		{
			throw new PeerError("File already exists");
		}
		receiveFile(file);
	}
	
	void receiveFile(File file) throws Exception
	{
		FileOutputStream stream = null;
		try 
		{
			Integer n_chunks = DBS.getDatabase().getNumberChunks(fileId);
			if (n_chunks == null) throw new PeerError("Unknown number of chunks");
			
			ExecutorService pool = Executors.newFixedThreadPool(MAX_THREADS);
			
			ArrayList<Future<Chunk>> list = new ArrayList<Future<Chunk>>();
			
			for (int chunkN = 0; chunkN < n_chunks; chunkN++)
			{
				ChunkRestore callable = new ChunkRestore(fileId, chunkN);
				Future<Chunk> future = pool.submit(callable);
				list.add(future);
			}
			for (Future<Chunk> future : list)
			{
				try
				{
					Chunk chunk = future.get();
					if (chunk == null)
					{
						if (stream != null)
							throw new PeerError("Restore Failed. File was partially restored");
						throw new PeerError("Restore failed completely.");
					}
					if (stream == null) stream = new FileOutputStream(file);
					stream.write(chunk.getChunkData());
				}
				catch (ExecutionException ex)
				{
					throw (Exception)ex.getCause();
				}
				try {
					Thread.sleep(19);
				} catch (Exception ex) {}
				if (!DBS.isRunning()) throw new PeerError("Server stopped");
			}
			System.out.println("File restored successfully");	
		} 
		catch (IOException ex) {
			ex.printStackTrace();
		} 
		finally {
			try {
				if (stream != null) stream.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new PeerError("Unexpected error");
			}
		}
	}
}
