package server.filesystem;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import server.main.DBS;
import server.messages.ChunkID;

public class Database implements Serializable{
	private static final long serialVersionUID = 4419138873746909853L;
	
	// Chunks received
	private HashSet<ChunkID> receivedBackups = new HashSet<ChunkID>();
	
	// Chunks received per fileId
	private HashMap<String, HashSet<Integer>> receivedFilesMap = new HashMap<String, HashSet<Integer>>();
	
	// FileIds per filename
	private HashMap<String, ArrayList<String>> sentBackups = new HashMap<String, ArrayList<String>>();
	
	// Every known chunk information
	private HashMap<ChunkID, ChunkInfo> chunksInfo = new HashMap<ChunkID, ChunkInfo>();
	
	// Set of files owned by this peer
	private HashSet<String> myFiles = new HashSet<String>();
	
	// Set of deleted files previously owned by this peer
	private HashSet<String> myDeletedFiles = new HashSet<String>();
	
	public void addChunkPeer(ChunkID chunkID, Integer peerID)
	{
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkID);
			if (info == null)
			{
				info = new ChunkInfo();
				chunksInfo.put(chunkID, info);
			}
			info.getPeers().add(peerID);
		}
		saveToFile();
	}
	
	public void removeChunkPeer(ChunkID chunkID, int sender) {
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkID);
			if (info != null)
				info.getPeers().remove(sender);
		}
		saveToFile();
	}
	
	public void addReceivedBackup(ChunkID chunkId, int size, int replication)
	{
		String fileId = chunkId.getFileId();
		synchronized (receivedFilesMap) {
			HashSet<Integer> fileChunks = receivedFilesMap.get(fileId);
			if (fileChunks == null)
			{
				receivedFilesMap.put(fileId, new HashSet<Integer>());
				fileChunks = receivedFilesMap.get(fileId);
			}
			fileChunks.add(chunkId.getNumber());
			synchronized (chunksInfo) {
				ChunkInfo info = chunksInfo.get(chunkId);
				if (info == null)
					chunksInfo.put(chunkId,new ChunkInfo(size,replication));
				else if (info.getSize() == null)
					info.setInfo(size, replication);
				synchronized (receivedBackups) {
					receivedBackups.add(chunkId);
				}
			}
			addChunkPeer(chunkId, DBS.getId());
		}
		saveToFile();
	}
	
	public void addChunkInfo(ChunkID chunkId, int size, int replication)
	{
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkId);
			if (info == null)
				chunksInfo.put(chunkId,new ChunkInfo(size,replication));
			else if (info.getSize() == null)
				info.setInfo(size, replication);
		}
		saveToFile();
	}
	
	public boolean hasBackup(ChunkID chunkID)
	{
		synchronized (receivedBackups) {
			return receivedBackups.contains(chunkID);
		}
	}
	
	public int getChunkCurrentReplication(ChunkID chunkID)
	{
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkID);
			if (info == null) return 0;
			return info.getPeers().size();
		}
	}
	
	public Integer getChunkDesiredReplication(ChunkID chunkID)
	{
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkID);
			if (info == null) return null;
			return info.getDesiredReplication();
		}
	}
	
	public long getTotalUsedSpace()
	{
		synchronized (chunksInfo) {
			synchronized (receivedBackups) {
				long sum = 0;
				for (ChunkID chunkID : receivedBackups)
				{
					sum += chunksInfo.get(chunkID).getSize();
				}
				return sum;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private HashMap<ChunkID, ChunkInfo> cloneChunksInfo()
	{
		synchronized (chunksInfo) {
			return (HashMap<ChunkID, ChunkInfo>)chunksInfo.clone();
		}
	}
	
	public void resetChunkReplication(ChunkID chunkID)
	{
		synchronized (chunksInfo) {
			ChunkInfo info = chunksInfo.get(chunkID);
			if (info != null) info.resetReplication();
		}
		synchronized (receivedBackups) {
			if (receivedBackups.contains(chunkID))
			{
				addChunkPeer(chunkID, DBS.getId());
			}
		}
	}
	
	public SortedSet<ChunkInfo> getBackupChunksInfo()
	{
		SortedSet<ChunkInfo> result = new TreeSet<ChunkInfo>();
		HashMap<ChunkID, ChunkInfo> map = cloneChunksInfo();
		for (Entry<ChunkID, ChunkInfo> entry : map.entrySet())
		{
			boolean backupReceived;
			synchronized (receivedBackups) {
				backupReceived = receivedBackups.contains(entry.getKey());
			}
			if (backupReceived)
				result.add(new ChunkInfo(entry.getKey(), entry.getValue()));
		}
		return result;
	}
	
	
	public void addSentBackup(String filename, String fileId)
	{
		synchronized (sentBackups) {
			ArrayList<String> list = sentBackups.get(filename);
			if (list == null) {
				sentBackups.put(filename, new ArrayList<String>());
				list = sentBackups.get(filename);
			}
			list.add(fileId);
		}
		synchronized (myFiles) {
			myFiles.add(fileId);
		}
		saveToFile();
	}
	
	public boolean isMyFile(String fileId)
	{
		synchronized (myFiles) {
			return myFiles.contains(fileId);
		}
	}
	
	public boolean isMyDeletedFile(String fileId) 
	{
		synchronized (fileId) {
			return myDeletedFiles.contains(fileId);
		}
	}
	
	public void deleteMyFile(String filename, String fileId)
	{
		synchronized (myDeletedFiles) {
			myDeletedFiles.add(fileId);
		}
		synchronized (sentBackups) {
			sentBackups.remove(filename);
		}
		synchronized (myFiles) {
			myFiles.remove(fileId);
		}
		saveToFile();
	}
	
	public String getLastSentFileId(String filename)
	{
		synchronized (sentBackups) {
			ArrayList<String> list = sentBackups.get(filename);
			if (list == null || list.isEmpty()) return null;
			return list.get(list.size()-1);
		}
	}
	
	public void removeReceivedBackup(ChunkID chunkId, boolean isDelete)
	{
		synchronized (receivedFilesMap) {
			String fileId = chunkId.getFileId();
			HashSet<Integer> chunks = receivedFilesMap.get(fileId);
			if (chunks == null) return;
			chunks.remove(chunkId.getNumber());
			if (chunks.isEmpty())
				receivedFilesMap.remove(fileId);
			if (isDelete)
			{
				synchronized (chunksInfo) {
					chunksInfo.remove(chunkId);
				}
			}
			receivedBackups.remove(chunkId);
		}
		removeChunkPeer(chunkId, DBS.getId());
		saveToFile();
	}
	
	public Set<Integer> getFileChunks(String fileId)
	{
		Set<Integer> result;
		synchronized (receivedFilesMap) {
			result = receivedFilesMap.get(fileId);
		}
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
