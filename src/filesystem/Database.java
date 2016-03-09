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
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import main.DBS;
import messages.ChunkID;

public class Database implements Serializable{
	private static final long serialVersionUID = 4419138873746909853L;
	
	// Chunks received
	private HashSet<ChunkID> receivedBackups = new HashSet<ChunkID>();
	
	// Chunks received per fileId
	private HashMap<String, HashSet<Integer>> receivedFilesMap = new HashMap<String, HashSet<Integer>>();
	
	// FileIds per filename
	private HashMap<String, HashSet<String>> sentBackups = new HashMap<String, HashSet<String>>();
	
	// Every known chunk information
	private HashMap<ChunkID, ChunkInfo> chunksInfo = new HashMap<ChunkID, ChunkInfo>();
	
	public synchronized void addChunkPeer(ChunkID chunkID, Integer peerID)
	{
		ChunkInfo info = chunksInfo.get(chunkID);
		if (info == null)
		{
			info = new ChunkInfo();
			chunksInfo.put(chunkID, info);
		}
		info.getPeers().add(peerID);
		saveToFile();
	}
	
	public void removeChunkPeer(ChunkID chunkID, int sender) {
		ChunkInfo info = chunksInfo.get(chunkID);
		if (info != null)
		{
			info.getPeers().remove(sender);
			saveToFile();
		}
	}
	
	public synchronized void addReceivedBackup(ChunkID chunkId, int size, int replication)
	{
		String fileId = chunkId.getFileId();
		HashSet<Integer> fileChunks = receivedFilesMap.get(fileId);
		if (fileChunks == null)
		{
			receivedFilesMap.put(fileId, new HashSet<Integer>());
			fileChunks = receivedFilesMap.get(fileId);
		}
		fileChunks.add(chunkId.getNumber());
		ChunkInfo info = chunksInfo.get(chunkId);
		if (info == null)
			chunksInfo.put(chunkId,new ChunkInfo(size,replication));
		else if (info.getSize() == null)
			info.setInfo(size, replication);
		receivedBackups.add(chunkId);
		addChunkPeer(chunkId, DBS.getId());
		saveToFile();
	}
	
	public synchronized void addChunkInfo(ChunkID chunkId, int size, int replication)
	{
		ChunkInfo info = chunksInfo.get(chunkId);
		if (info == null)
			chunksInfo.put(chunkId,new ChunkInfo(size,replication));
		else if (info.getSize() == null)
			info.setInfo(size, replication);
		saveToFile();
	}
	
	public synchronized boolean hasBackup(ChunkID chunkID)
	{
		return receivedBackups.contains(chunkID);
	}
	
	public synchronized int getChunkReplication(ChunkID chunkID)
	{
		ChunkInfo info = chunksInfo.get(chunkID);
		if (info == null) return 0;
		return info.getPeers().size();
	}
	
	public synchronized long getTotalUsedSpace()
	{
		long sum = 0;
		for (ChunkID chunkID : receivedBackups)
		{
			sum += chunksInfo.get(chunkID).getSize();
		}
		return sum;
	}
	
	@SuppressWarnings("unchecked")
	private synchronized HashMap<ChunkID, ChunkInfo> cloneChunksInfo()
	{
		return (HashMap<ChunkID, ChunkInfo>)chunksInfo.clone();
	}
	
	public SortedSet<ChunkInfo> getSortedChunksInfo()
	{
		SortedSet<ChunkInfo> result = new TreeSet<ChunkInfo>();
		HashMap<ChunkID, ChunkInfo> map = cloneChunksInfo();
		for (Entry<ChunkID, ChunkInfo> entry : map.entrySet())
		{
			result.add(new ChunkInfo(entry.getKey(), entry.getValue()));
		}
		return result;
	}
	
	
	public synchronized void addSentBackup(String filename, String fileId)
	{
		Set<String> set = sentBackups.get(filename);
		if (set == null) {
			sentBackups.put(filename, new HashSet<String>());
			set = sentBackups.get(filename);
		}
		set.add(fileId);
		saveToFile();
	}
	
	public synchronized Set<String> getSentFileIds(String filename)
	{
		return sentBackups.get(filename);
	}
	
	public synchronized void removeReceivedBackup(ChunkID chunkId, boolean isDelete)
	{
		String fileId = chunkId.getFileId();
		HashSet<Integer> chunks = receivedFilesMap.get(fileId);
		if (chunks == null) return;
		chunks.remove(chunkId.getNumber());
		if (chunks.isEmpty())
			receivedFilesMap.remove(fileId);
		if (isDelete)
			chunksInfo.remove(chunkId);
		receivedBackups.remove(chunkId);
		removeChunkPeer(chunkId, DBS.getId());
		saveToFile();
	}
	
	public synchronized Set<Integer> getFileChunks(String fileId)
	{
		Set<Integer> result = receivedFilesMap.get(fileId);
		saveToFile();
		return result;
	}
	
	private synchronized void saveToFile()
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
