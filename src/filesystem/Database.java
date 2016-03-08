package filesystem;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import messages.ChunkID;

public class Database implements Serializable{
	private static final long serialVersionUID = 4419138873746909853L;
	
	// Chunks received
	private HashSet<ChunkID> receivedBackups = new HashSet<ChunkID>();
	
	// Chunks received per file
	private HashMap<String, HashSet<Integer>> receivedFilesMap = new HashMap<String, HashSet<Integer>>();
	
	// Filename to set of fileIds
	private HashMap<String, HashSet<String>> sentBackups = new HashMap<String, HashSet<String>>();
	
	public void addReceivedBackup(ChunkID chunkId)
	{
		String fileId = chunkId.getFileId();
		HashSet<Integer> fileChunks = receivedFilesMap.get(fileId);
		if (fileChunks == null)
		{
			receivedFilesMap.put(fileId, new HashSet<Integer>());
			fileChunks = receivedFilesMap.get(fileId);
		}
		fileChunks.add(chunkId.getNumber());
		receivedBackups.add(chunkId);
		saveToFile();
	}
	
	public void addSentBackup(String filename, String fileId)
	{
		Set<String> set = sentBackups.get(filename);
		if (set == null) {
			sentBackups.put(filename, new HashSet<String>());
			set = sentBackups.get(filename);
		}
		set.add(fileId);
		saveToFile();
	}
	
	public Set<String> getSentFileIds(String filename)
	{
		return sentBackups.get(filename);
	}
	
	public void removeReceivedBackup(ChunkID chunkId)
	{
		String fileId = chunkId.getFileId();
		HashSet<Integer> chunks = receivedFilesMap.get(fileId);
		if (chunks == null) return;
		chunks.remove(chunkId.getNumber());
		if (chunks.isEmpty()) receivedFilesMap.remove(fileId);
		receivedBackups.remove(chunkId);
		saveToFile();
	}
	
	public Set<Integer> getFileChunks(String fileId)
	{
		Set<Integer> result = receivedFilesMap.get(fileId);
		saveToFile();
		return result;
	}
	
	private void saveToFile()
	{
		try (FileOutputStream fileOut = new FileOutputStream("database.db");)
		{
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this);
	        out.close();
		}
		catch (IOException ex)
		{
			ex.printStackTrace();
			System.out.println("Could not write database");
		}
	}
	
	public static Database fromFile()
	{
		Database database = null;
		try (FileInputStream fileIn = new FileInputStream("database.db");
				ObjectInputStream in = new ObjectInputStream(fileIn);)
		{
			database = (Database)in.readObject();
		}catch(IOException i)
		{
			if (i instanceof FileNotFoundException)
			{
				System.out.println("Database does not exist");
				return null;
			}
			i.printStackTrace();
			return null;
		}catch(ClassNotFoundException c)
		{
			c.printStackTrace();
			return null;
		}
		return database;
	}
}
