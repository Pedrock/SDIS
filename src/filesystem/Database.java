package filesystem;

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
	}
	
	public void addSentBackup(String filename, String fileId)
	{
		Set<String> set = sentBackups.get(filename);
		if (set == null) {
			sentBackups.put(filename, new HashSet<String>());
			set = sentBackups.get(filename);
		}
		set.add(fileId);
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
	}
	
	public Set<Integer> getFileChunks(String fileId)
	{
		return receivedFilesMap.get(fileId);
	}
}
