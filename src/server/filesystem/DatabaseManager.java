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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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

	private AtomicBoolean needsSaving = new AtomicBoolean(false);
	private ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
	private ReadLock readLock = readWriteLock.readLock();
	private WriteLock writeLock = readWriteLock.writeLock();

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
		readLock.lock();
		try {
			synchronized (db.backup_space) {
				return db.backup_space;
			}
		}
		finally {
			readLock.unlock();
		}
	}

	public void setBackupSpace(long backup_space)
	{
		readLock.lock();
		try {
			synchronized (db.backup_space) {
				db.backup_space = backup_space;
			}
		} 
		finally {
			readLock.unlock();
		}
	}
	
	private void addChunkPeer(ChunkID chunkID, String peerID, boolean save)
	{
		readLock.lock();
		try {
			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkID);
				if (info == null)
				{
					info = new ChunkInfo();
					db.chunksInfo.put(chunkID, info);
				}
				info.getPeers().add(peerID);
			}
		} 
		finally {
			readLock.unlock();
		}
		if (save) saveToFile();
	}

	public void addChunkPeer(ChunkID chunkID, String peerID)
	{
		addChunkPeer(chunkID, peerID, true);
	}
	
	private void removeChunkPeer(ChunkID chunkID, String sender, boolean save)
	{
		readLock.lock();
		try {
			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkID);
				if (info != null)
					info.getPeers().remove(sender);
			}
		} 
		finally {
			readLock.unlock();
		}
		if (save) saveToFile();
	}

	public void removeChunkPeer(ChunkID chunkID, String sender) {
		removeChunkPeer(chunkID, sender, true);
	}

	public void addReceivedBackup(ChunkID chunkId, int size, int replication)
	{
		readLock.lock();
		try {
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
		} 
		finally {
			readLock.unlock();
		}
		saveToFile();
	}

	public void addChunkInfo(ChunkID chunkId, int size, int replication)
	{
		readLock.lock();
		try {

			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkId);
				if (info == null)
					db.chunksInfo.put(chunkId,new ChunkInfo(size,replication));
				else if (info.getSize() == null)
					info.setInfo(size, replication);
			}
		} 
		finally {
			readLock.unlock();
		}
		saveToFile();
	}

	public boolean hasBackup(ChunkID chunkID)
	{
		readLock.lock();
		try {
			synchronized (db.receivedBackups) {
				return db.receivedBackups.contains(chunkID);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public int getChunkCurrentReplication(ChunkID chunkID)
	{
		readLock.lock();
		try {
			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkID);
				if (info == null) return 0;
				return info.getPeers().size();
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public Integer getChunkDesiredReplication(ChunkID chunkID)
	{
		readLock.lock();
		try {
			synchronized (db.chunksInfo) {
				ChunkInfo info = db.chunksInfo.get(chunkID);
				if (info == null) return null;
				return info.getDesiredReplication();
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public long getTotalUsedSpace()
	{
		readLock.lock();
		try {
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
		finally {
			readLock.unlock();
		}
	}

	@SuppressWarnings("unchecked")
	private HashMap<ChunkID, ChunkInfo> cloneChunksInfo()
	{
		readLock.lock();
		try {
			synchronized (db.chunksInfo) {
				return (HashMap<ChunkID, ChunkInfo>)db.chunksInfo.clone();
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public void resetChunkReplication(ChunkID chunkID)
	{
		boolean save = false;
		readLock.lock();
		try {
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
		} 
		finally {
			readLock.unlock();
			if (save) saveToFile();
		}
	}

	public SortedSet<ChunkInfo> getBackupChunksInfo()
	{
		readLock.lock();
		try {
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
		finally {
			readLock.unlock();
		}
	}


	public void addSentBackup(String filename, String fileId, int n_chunks)
	{
		readLock.lock();
		try {
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
		} 
		finally {
			readLock.unlock();
		}
		saveToFile();
	}

	public Integer getNumberChunks(String fileId)
	{
		readLock.lock();
		try {
			return db.myFiles.get(fileId);
		} 
		finally {
			readLock.unlock();
		}

	}

	public boolean isMyFile(String fileId)
	{
		readLock.lock();
		try {
			synchronized (db.myFiles) {
				return db.myFiles.containsKey(fileId);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public boolean isMyDeletedFile(String fileId) 
	{
		readLock.lock();
		try {
			synchronized (db.myDeletedFiles) {
				return db.myDeletedFiles.contains(fileId);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public void removeDeletion(String fileId)
	{
		readLock.lock();
		try {
			synchronized (db.myDeletedFiles) {
				db.myDeletedFiles.remove(fileId);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public void deleteMyFile(String filename, String fileId)
	{
		readLock.lock();
		try {
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
		} 
		finally {
			readLock.unlock();
		}
		saveToFile();
	}

	public String getLastSentFileId(String filename)
	{
		readLock.lock();
		try {
			synchronized (db.sentBackups) {
				ArrayList<String> list = db.sentBackups.get(filename);
				if (list == null || list.isEmpty()) return null;
				return list.get(list.size()-1);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public void removeReceivedBackup(ChunkID chunkId, boolean isDelete)
	{
		readLock.lock();
		try {
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
			removeChunkPeer(chunkId, DBS.getId(), false);
		} 
		finally {
			readLock.unlock();
		}
		saveToFile();
	}

	public Set<Integer> getFileChunks(String fileId)
	{
		readLock.lock();
		try {
			synchronized (db.receivedFilesMap) {
				return db.receivedFilesMap.get(fileId);
			}
		} 
		finally {
			readLock.unlock();
		}
	}

	public boolean getchunkReceived(ChunkID chunkID)
	{
		readLock.lock();
		try {
			boolean result;
			synchronized (getchunkTimes) {
				result = getchunkTimes.containsKey(chunkID);
				getchunkTimes.put(chunkID, Instant.now().getEpochSecond());
			}
			return result;
		} 
		finally {
			readLock.unlock();
		}
	}

	private void saveToFile()
	{
		needsSaving.set(true);
		writeLock.lock();
		if (!needsSaving.get()) return;
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
		finally {
			needsSaving.set(false);
			writeLock.unlock();
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
