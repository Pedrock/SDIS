package server.tasks;

import java.io.File;
import java.util.Random;
import java.util.SortedSet;

import server.filesystem.ChunkInfo;
import server.main.DBS;
import server.messages.Chunk;
import server.messages.ChunkID;

public class SpaceReclaiming implements Runnable {
	
	private long backup_space;
	private Random random = new Random();
	
	public SpaceReclaiming(long backup_space) {
		this.backup_space = backup_space;
	}
	
	
	public void run() {
		DBS.setBackupSpace(backup_space);
		SortedSet<ChunkInfo> infos = DBS.getDatabase().getBackupChunksInfo();
		long usedSpace = DBS.getDatabase().getTotalUsedSpace();
		System.out.println(usedSpace);
		for (ChunkInfo info : infos)
		{
			if (usedSpace <= backup_space) break; // Objective achieved
			ChunkID chunkID = info.getChunkID();
			
			DBS.getMessageBuilder().sendRemoved(chunkID.getFileId(),chunkID.getNumber());
			if (info.getOverReplication() > 0)
			{
				usedSpace -= deleteChunk(info);
			}
			else
			{
				Integer size = handleLowReplication(info);
				if (size != null) usedSpace -= size;
				else return;
			}
			
			int delay = random.nextInt(51); // [0,50]
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) { }
		}
		System.out.println("Space reclaimed successfully.");
	}
	
	private int deleteChunk(ChunkInfo info)
	{
		ChunkID chunkID = info.getChunkID();
		File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
		file.delete();
		DBS.getDatabase().removeReceivedBackup(chunkID, false);
		return info.getSize();
	}
	
	private Integer handleLowReplication(ChunkInfo info)
	{
		ChunkID chunkID = info.getChunkID();
		DBS.getMdbListener().notifyOnPutChunk(this, chunkID);
		
		int delay = random.nextInt(501); // [0,500]
		synchronized(this) {
			try {
				wait(delay);
			} catch (InterruptedException e) {}
		}
		
		int replication = info.getDesiredReplication();
		
		if (!DBS.getMdbListener().putChunkReceived(chunkID))
		{
			System.out.println("Sending chunk content for: "+chunkID);
			byte[] data = DBS.getBackupsFileManager().getChunkContent(chunkID);
			new BackupChunk(new Chunk(chunkID, data, replication)).run();
		}
		
		if (DBS.getDatabase().getChunkCurrentReplication(chunkID) < replication)
		{
			System.out.println("Could not complete space reclaiming, not enough replication.");
			return null;
		}
		return deleteChunk(info);
	}
}