package server.filesystem;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import server.main.DBS;
import server.messages.ChunkID;

public class DatabaseManager{
	
	static private class DB implements Serializable {
		private static final long serialVersionUID = -8881389891970410854L;

		// Chunks received
		private HashSet<ChunkID> receivedBackups = new HashSet<ChunkID>();
		
		// fileID -> chunkNumbers
		private HashMap<String, HashSet<Integer>> receivedFilesMap = new HashMap<String, HashSet<Integer>>();
		
		// filename -> fileID
		private HashMap<String, ArrayList<String>> sentBackups = new HashMap<String, ArrayList<String>>();
		
		// Every known chunk information
		private HashMap<ChunkID, ChunkInfo> chunksInfo = new HashMap<ChunkID, ChunkInfo>();
		
		// fileIDs owned by this peer -> number of chunks
		private HashMap<String,Integer> myFiles = new HashMap<String,Integer>();
		
		// Set of deleted fileIDs previously owned by this peer
		private HashSet<String> myDeletedFiles = new HashSet<String>();
		
		// Max backup space
		private Long backup_space = 3200000L;
	};
	
	private DB db;

	private HashMap<ChunkID,Long> getchunkTimes = new HashMap<ChunkID,Long>();
	
	private Thread thread = new Thread() {
		@Override
		public void run() {
			boolean running = true;
			while (running)
			{
				ArrayList<ChunkID> deletes = new ArrayList<ChunkID>();
				synchronized (getchunkTimes) {
					long delete_time = Instant.now().getEpochSecond()+60;
					for (Entry<ChunkID,Long> entry : getchunkTimes.entrySet())
					{
						if (entry.getValue() >= delete_time)
							deletes.add(entry.getKey());
					}
					for (ChunkID chunkID : deletes)
					{
						getchunkTimes.remove(chunkID);
					}
				}
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					running = false;
				}
			}
		}
	};
	
	public DatabaseManager(DB db)
	{
		this.db = db;
		thread.start();
	}
	
	public DatabaseManager()
	{
		this.db = new DB();
		thread.start();
	}
	
	public void interruptThread()
	{
		thread.interrupt();
	}
	
	public long getBackupSpace()
	{
		synchronized (db.backup_space) {
			return db.backup_space;
		}
	}
	
	public void setBackupSpace(long backup_space)
	{
		synchronized (db.backup_space) {
			db.backup_space = backup_space;
		}
	}
	
	public void addChunkPeer(ChunkID chunkID, String peerID)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info == null)
			{
				info = new ChunkInfo();
				db.chunksInfo.put(chunkID, info);
			}
			info.getPeers().add(peerID);
		}
		saveToFile();
	}
	
	public void removeChunkPeer(ChunkID chunkID, String sender) {
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info != null)
				info.getPeers().remove(sender);
		}
		saveToFile();
	}
	
	public void addReceivedBackup(ChunkID chunkId, int size, int replication)
	{
		String fileId = chunkId.getFileId();
		synchronized (db.receivedFilesMap) {
			HashSet<Integer> fileChunks = db.receivedFilesMap.get(fileId);
			if (fileChunks == null)
			{
				db.receivedFilesMap.put(fileId, new HashSet<Integer>());
				fileChunks = db.receivedFilesMap.get(fileId);
			}
			fileChunks.add(chunkId.getNumber());
			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkId);
				if (info == null)
					db.chunksInfo.put(chunkId,new ChunkInfo(size,replication));
				else if (info.getSize() == null)
					info.setInfo(size, replication);
				synchronized (db.receivedBackups) {
					db.receivedBackups.add(chunkId);
				}
			}
			addChunkPeer(chunkId, DBS.getId());
		}
		saveToFile();
	}
	
	public void addChunkInfo(ChunkID chunkId, int size, int replication)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkId);
			if (info == null)
				db.chunksInfo.put(chunkId,new ChunkInfo(size,replication));
			else if (info.getSize() == null)
				info.setInfo(size, replication);
		}
		saveToFile();
	}
	
	public boolean hasBackup(ChunkID chunkID)
	{
		synchronized (db.receivedBackups) {
			return db.receivedBackups.contains(chunkID);
		}
	}
	
	public int getChunkCurrentReplication(ChunkID chunkID)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info == null) return 0;
			return info.getPeers().size();
		}
	}
	
	public Integer getChunkDesiredReplication(ChunkID chunkID)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info == null) return null;
			return info.getDesiredReplication();
		}
	}
	
	public long getTotalUsedSpace()
	{
		synchronized (db.chunksInfo) {
			synchronized (db.receivedBackups) {
				long sum = 0;
				for (ChunkID chunkID : db.receivedBackups)
				{
					sum += db.chunksInfo.get(chunkID).getSize();
				}
				return sum;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private HashMap<ChunkID, ChunkInfo> cloneChunksInfo()
	{
		synchronized (db.chunksInfo) {
			return (HashMap<ChunkID, ChunkInfo>)db.chunksInfo.clone();
		}
	}
	
	public void resetChunkReplication(ChunkID chunkID)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info != null) info.resetReplication();
		}
		synchronized (db.receivedBackups) {
			if (db.receivedBackups.contains(chunkID))
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
			synchronized (db.receivedBackups) {
				backupReceived = db.receivedBackups.contains(entry.getKey());
			}
			if (backupReceived)
				result.add(new ChunkInfo(entry.getKey(), entry.getValue()));
		}
		return result;
	}
	
	
	public void addSentBackup(String filename, String fileId, int n_chunks)
	{
		synchronized (db.sentBackups) {
			ArrayList<String> list = db.sentBackups.get(filename);
			if (list == null) {
				db.sentBackups.put(filename, new ArrayList<String>());
				list = db.sentBackups.get(filename);
			}
			list.add(fileId);
		}
		synchronized (db.myFiles) {
			db.myFiles.put(fileId,n_chunks);
		}
		saveToFile();
	}
	
	public Integer getNumberChunks(String fileId)
	{
		return db.myFiles.get(fileId);
	}
	
	public boolean isMyFile(String fileId)
	{
		synchronized (db.myFiles) {
			return db.myFiles.containsKey(fileId);
		}
	}
	
	public boolean isMyDeletedFile(String fileId) 
	{
		synchronized (db.myDeletedFiles) {
			return db.myDeletedFiles.contains(fileId);
		}
	}
	
	public void deleteMyFile(String filename, String fileId)
	{
		Set<Integer> chunksSet = this.getFileChunks(fileId);
		if (chunksSet != null) {
			Integer[] chunks = chunksSet.toArray(new Integer[chunksSet.size()]);
			if (chunks != null)
			{
				for (Integer chunk : chunks) 
				{
					ChunkID chunkId = new ChunkID(fileId, chunk);
					synchronized (db.chunksInfo) {
						db.chunksInfo.remove(chunkId);
					}
				}
			}
		}
		synchronized (db.myDeletedFiles) {
			db.myDeletedFiles.add(fileId);
		}
		synchronized (db.sentBackups) {
			db.sentBackups.remove(filename);
		}
		synchronized (db.myFiles) {
			db.myFiles.remove(fileId);
		}
		saveToFile();
	}
	
	public String getLastSentFileId(String filename)
	{
		synchronized (db.sentBackups) {
			ArrayList<String> list = db.sentBackups.get(filename);
			if (list == null || list.isEmpty()) return null;
			return list.get(list.size()-1);
		}
	}
	
	public void removeReceivedBackup(ChunkID chunkId, boolean isDelete)
	{
		synchronized (db.receivedFilesMap) {
			String fileId = chunkId.getFileId();
			HashSet<Integer> chunks = db.receivedFilesMap.get(fileId);
			if (chunks == null) return;
			chunks.remove(chunkId.getNumber());
			if (chunks.isEmpty())
				db.receivedFilesMap.remove(fileId);
			if (isDelete)
			{
				synchronized (db.chunksInfo) {
					db.chunksInfo.remove(chunkId);
				}
			}
			db.receivedBackups.remove(chunkId);
		}
		removeChunkPeer(chunkId, DBS.getId());
		saveToFile();
	}
	
	public Set<Integer> getFileChunks(String fileId)
	{
		Set<Integer> result;
		synchronized (db.receivedFilesMap) {
			result = db.receivedFilesMap.get(fileId);
		}
		saveToFile();
		return result;
	}
	
	public boolean getchunkReceived(ChunkID chunkID)
	{
		boolean result;
		synchronized (getchunkTimes) {
			result = getchunkTimes.containsKey(chunkID);
			getchunkTimes.put(chunkID, Instant.now().getEpochSecond());
		}
		return result;
	}
	
	private synchronized void saveToFile()
	{
		File file = Paths.get(DBS.getId(), "database.db").toFile();
		try (FileOutputStream fileOut = new FileOutputStream(file);)
		{
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(this.db);
	        out.close();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			System.out.println("Could not write database");
		}
	}
	
	public static DatabaseManager fromFile()
	{
		DB db = null;
		File file = Paths.get(DBS.getId(), "database.db").toFile();
		try (FileInputStream fileIn = new FileInputStream(file);
				ObjectInputStream in = new ObjectInputStream(fileIn);)
		{
			db = (DB)in.readObject();
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
		DatabaseManager databaseManager = new DatabaseManager(db);
		return databaseManager;
	}
}
