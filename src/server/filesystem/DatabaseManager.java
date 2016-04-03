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
		private HashSet<ChunkID> receivedBackups = null;

		// fileID -> chunkNumbers
		private HashMap<String, HashSet<Integer>> receivedFilesMap = null;

		// filename -> fileID
		private HashMap<String, ArrayList<String>> sentBackups = null;

		// Every known chunk information
		private HashMap<ChunkID, ChunkInfo> chunksInfo = null;

		// fileIDs owned by this peer -> number of chunks
		private HashMap<String,Integer> myFiles = null;

		// Set of deleted fileIDs previously owned by this peer
		private HashSet<String> myDeletedFiles = null;

		// Max backup space
		private Long backup_space = null;

		private void constructor()
		{
			if (receivedBackups == null) receivedBackups = new HashSet<ChunkID>();
			if (receivedFilesMap == null) receivedFilesMap = new HashMap<String, HashSet<Integer>>();
			if (sentBackups == null) sentBackups = new HashMap<String, ArrayList<String>>();
			if (chunksInfo == null) chunksInfo = new HashMap<ChunkID, ChunkInfo>();
			if (myFiles == null) myFiles = new HashMap<String,Integer>();
			if (myDeletedFiles == null) myDeletedFiles = new HashSet<String>();
			if (backup_space == null) backup_space = 3200000L;
		}

		public DB()
		{
			constructor();
		}

		@SuppressWarnings("unchecked")
		public DB(Object backup_space, Object chunksInfo, Object myDeletedFiles, Object myFiles,
				Object receivedBackups, Object receivedFilesMap, Object sentBackups) {
			this.backup_space = (Long) backup_space;
			this.chunksInfo = (HashMap<ChunkID, ChunkInfo>) chunksInfo;
			this.myDeletedFiles = (HashSet<String>) myDeletedFiles;
			this.myFiles = (HashMap<String, Integer>) myFiles;
			this.receivedBackups = (HashSet<ChunkID>) receivedBackups;
			this.receivedFilesMap = (HashMap<String, HashSet<Integer>>) receivedFilesMap;
			this.sentBackups = (HashMap<String, ArrayList<String>>) sentBackups;
			constructor();
		}
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
		saveToFile(db.backup_space);
	}

	private void addChunkPeer(ChunkID chunkID, String peerID, boolean save)
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
		if (save) saveToFile(db.chunksInfo);
	}

	public void addChunkPeer(ChunkID chunkID, String peerID)
	{
		addChunkPeer(chunkID, peerID, true);
	}

	private void removeChunkPeer(ChunkID chunkID, String sender, boolean save)
	{
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info != null)
				info.getPeers().remove(sender);
		}
		if (save) saveToFile(db.chunksInfo);
	}

	public void removeChunkPeer(ChunkID chunkID, String sender) {
		removeChunkPeer(chunkID, sender, true);
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
			addChunkPeer(chunkId, DBS.getId(), false);
		}
		saveToFile(db.receivedFilesMap, db.chunksInfo, db.receivedBackups);
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

		saveToFile(db.chunksInfo);
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
		boolean save = false;
		synchronized (db.chunksInfo) {
			ChunkInfo info = db.chunksInfo.get(chunkID);
			if (info != null) info.resetReplication();
		}
		synchronized (db.receivedBackups) {
			if (db.receivedBackups.contains(chunkID))
			{
				save = true;
				addChunkPeer(chunkID, DBS.getId(), false);
			}
		}
		if (save) saveToFile(db.chunksInfo, db.receivedBackups);
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
		saveToFile(db.sentBackups, db.myFiles);
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

	public void removeDeletion(String fileId)
	{
		synchronized (db.myDeletedFiles) {
			db.myDeletedFiles.remove(fileId);
		}
		saveToFile(db.myDeletedFiles);
	}

	public void deleteMyFile(String filename, String fileId)
	{
		Integer num_chunks = this.getNumberChunks(fileId);
		if (num_chunks != null) {
			for (int i = 0; i < num_chunks; i++) 
			{
				ChunkID chunkId = new ChunkID(fileId, i);
				synchronized (db.chunksInfo) {
					db.chunksInfo.remove(chunkId);
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
		saveToFile(db.chunksInfo, db.myDeletedFiles, db.sentBackups, db.myFiles);
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
		}
		if (isDelete)
		{
			synchronized (db.chunksInfo) {
				db.chunksInfo.remove(chunkId);
			}
			saveToFile(db.chunksInfo);
		}
		synchronized (db.receivedBackups) {
			db.receivedBackups.remove(chunkId);
		}
		removeChunkPeer(chunkId, DBS.getId(), false);
		saveToFile(db.receivedFilesMap, db.receivedBackups);
	}

	public Set<Integer> getFileChunks(String fileId)
	{
		synchronized (db.receivedFilesMap) {
			return db.receivedFilesMap.get(fileId);
		}
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

	private void saveToFile(Object object, String filename)
	{
		File file = Paths.get(DBS.getId(), filename).toFile();

		try (FileOutputStream fileOut = new FileOutputStream(file);)
		{
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			synchronized (object) {
				out.writeObject(object);
			}
			out.close();
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			System.out.println("Could not write database");
		}
	}

	private void saveToFile(Object object)
	{
		String filename = null;
		if (object == db.backup_space) filename = "backup_space";
		else if (object == db.chunksInfo) filename = "chunksInfo";
		else if (object == db.myDeletedFiles) filename = "myDeletedFiles";
		else if (object == db.myFiles) filename = "myFiles";
		else if (object == db.receivedBackups) filename = "receivedBackups";
		else if (object == db.receivedFilesMap) filename = "receivedFilesMap";
		else if (object == db.sentBackups) filename = "sentBackups";
		if (filename == null) return;
		filename += ".db";
		saveToFile(object, filename);
	}

	private void saveToFile(Object... objects)
	{
		for (Object object : objects)
		{
			saveToFile(object);
		}
	}

	private static Object fromFile(String filename)
	{
		File file = Paths.get(DBS.getId(), filename).toFile();
		try (FileInputStream fileIn = new FileInputStream(file);
				ObjectInputStream in = new ObjectInputStream(fileIn);)
		{
			return in.readObject();
		}catch(IOException i)
		{
			if (i instanceof FileNotFoundException)
			{
				return null;
			}
			i.printStackTrace();
			return null;
		}catch(ClassNotFoundException c)
		{
			c.printStackTrace();
			return null;
		}
	}

	public static DatabaseManager fromFile()
	{
		Object backup_space = fromFile("backup_space.db");
		Object chunksInfo = fromFile("chunksInfo.db");
		Object myDeletedFiles = fromFile("myDeletedFiles.db");
		Object myFiles = fromFile("myFiles.db");
		Object receivedBackups = fromFile("receivedBackups.db");
		Object receivedFilesMap = fromFile("receivedFilesMap.db");
		Object sentBackups = fromFile("sentBackups.db");

		DB db = new DB(backup_space, chunksInfo, myDeletedFiles, myFiles, receivedBackups, receivedFilesMap, sentBackups);

		DatabaseManager databaseManager = new DatabaseManager(db);
		return databaseManager;
	}
}
