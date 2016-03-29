package server.tasks;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import server.filesystem.FileManager;
import server.main.DBS;
import server.messages.Chunk;

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
			String fileId = localFM.generateFileHash(filename);
			boolean success = true;
			for (int n = 1; n <= chunks; n++)
			{
				long remaining_size = file_size - (n-1)*DBS.CHUNK_SIZE;
				int chunk_size = (int)Math.min(DBS.CHUNK_SIZE, remaining_size);
				byte[] chunk = new byte[chunk_size];
				s.read(chunk);
				BackupChunk task = new BackupChunk(new Chunk(fileId, n, chunk, replication));
				task.run();
				success = success && task.wasSuccessful();
			}
			DBS.getDatabase().addSentBackup(filename, fileId);
			if (success) System.out.println("Backup finished succesfully");
			else throw new Exception("Backup finished but the replication degree was not achieved");
		} catch (FileNotFoundException e) {
			throw new Exception("File does not exist");
		} catch (IOException e) {
			throw new Exception("Error while reading file");
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
