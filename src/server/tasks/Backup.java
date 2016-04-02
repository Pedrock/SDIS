package server.tasks;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import server.filesystem.FileManager;
import server.main.DBS;
import server.main.PeerError;
import server.messages.Chunk;
import server.messages.ChunkID;

public class Backup implements Runnable{
	
	private String filename;
	private int replication;
	
	
	public Backup(String filename, int replication) {
		this.filename = filename;
		this.replication = replication;
	}
	
	public void runWithExceptions() throws Exception
	{
		FileManager localFM = DBS.getLocalFileManager();
		File file = localFM.getFile(filename);
		
		long file_size = file.length();
		int chunks = (int)(file_size / DBS.CHUNK_SIZE) + 1;
		try (BufferedInputStream s = new BufferedInputStream(new FileInputStream(file)))
		{
			ExecutorService executorService = Executors.newCachedThreadPool();
			String fileId = localFM.generateFileHash(filename);
			boolean success = true;
			for (int n = 0; n < chunks; n++)
			{
				long remaining_size = file_size - n*DBS.CHUNK_SIZE;
				int chunk_size = (int)Math.min(DBS.CHUNK_SIZE, remaining_size);
				byte[] chunk = new byte[chunk_size];
				s.read(chunk);
				ChunkID chunkID = new ChunkID(fileId, n);
				DBS.getDatabase().addChunkInfo(chunkID, chunk_size, replication);
				BackupChunk callable = new BackupChunk(new Chunk(chunkID, chunk, replication));
				executorService.submit(callable);
				try {
					Thread.sleep(19);
				} catch (Exception ex) {}
				if (!DBS.isRunning()) throw new PeerError("Server stopped");
			}
			executorService.shutdown();
			executorService.awaitTermination(60, TimeUnit.SECONDS);
			DBS.getDatabase().addSentBackup(filename, fileId, chunks);
			if (success) System.out.println("Backup finished succesfully");
			else throw new PeerError("Backup finished but the replication degree was not achieved");
		} catch (FileNotFoundException e) {
			throw new PeerError("File does not exist");
		} catch (IOException e) {
			throw new PeerError("Error while reading file");
		}
	}
	
	@Override
	public void run() {
		try
		{
			runWithExceptions();
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
}
