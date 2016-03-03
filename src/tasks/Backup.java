package tasks;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import filesystem.FileManager;
import main.DBS;
import messages.Chunk;

public class Backup implements Runnable{
	
	private String filename;
	private int replication;
	
	
	public Backup(String filename, int replication) {
		this.filename = filename;
		this.replication = replication;
	}
	
	@Override
	public void run() {
		FileManager localFM = DBS.getLocalFileManager();
		File file = localFM.getFile(filename);
		
		long file_size = file.length();
		int chunks = (int)(file_size / DBS.CHUNK_SIZE) + 1;
		try (BufferedInputStream s = new BufferedInputStream(new FileInputStream(file)))
		{
			String fileId = localFM.generateFileHash(filename);
			for (int n = 1; n <= chunks; n++)
			{
				long remaining_size = file_size - (n-1)*DBS.CHUNK_SIZE;
				int chunk_size = (int)Math.min(DBS.CHUNK_SIZE, remaining_size);
				byte[] chunk = new byte[chunk_size];
				s.read(chunk);
				new BackupChunk(new Chunk(fileId, n, chunk, replication)).run();
			}
			System.out.println("Backup finished succesfully");
		} catch (FileNotFoundException e) {
			System.err.println("File does not exist");
		} catch (IOException e) {
			System.err.println("Error while reading file");
			e.printStackTrace();
		}
	}
}
