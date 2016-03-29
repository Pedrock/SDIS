package server.tasks;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.rmi.activation.UnknownObjectException;
import java.util.concurrent.TimeoutException;

import server.filesystem.FileManager;
import server.main.DBS;
import server.messages.Chunk;
import server.messages.ChunkID;

public class Restore implements Runnable{
	
	private static final int INITIAL_SLEEP = 500;
	private static final int MAX_TRIES = 6;
	
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
			throw new Exception("File already exists");
		}
		receiveFile(file);
	}
	
	void receiveFile(File file) throws Exception
	{
		FileOutputStream stream = null;
		try 
		{
			int chunkN = 1;
			int chunk_size = 0;
			do
			{
				int sleep = INITIAL_SLEEP;
				int n_try = 0;
				boolean chunk_received = false;
				for (; n_try < MAX_TRIES && !chunk_received; n_try++)
				{
					ChunkID chunkID = new ChunkID(fileId, chunkN);
					DBS.getMdrListener().notifyOnChunk(this, chunkID);
					
					DBS.getMessageBuilder().sendGetChunk(fileId, chunkN);
					synchronized(this) {
						try {
							wait(sleep);
						} catch (InterruptedException e) {}
					}
					sleep *= 2;
					Chunk chunk = DBS.getMdrListener().getChunk(chunkID);
					if (chunk != null)
					{
						chunk_received = true;
						chunkN++;
						System.out.println("Valid chunk received");
						chunk_size = chunk.getChunkData().length;
						if (stream == null) stream = new FileOutputStream(file);
						stream.write(chunk.getChunkData());
					}
				}
				if (n_try == 5)
				{
					throw new TimeoutException();
				}
			}
			while (chunk_size == DBS.CHUNK_SIZE);
			System.out.println("File restored successfully");	
		} 
		catch (IOException ex) {
			ex.printStackTrace();
		} catch (TimeoutException e) {
			if (stream != null)
				throw new Exception("Restore Failed. File was partially restored");
			throw new Exception("Restore failed completely.");
		}
		finally {
			try {
				if (stream != null) stream.close();
			} catch (IOException e) {
				e.printStackTrace();
				throw new Exception("Unexpected error");
			}
		}
	}
}
