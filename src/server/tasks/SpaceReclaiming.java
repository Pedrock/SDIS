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
			
			try {
				Thread.sleep(400);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			
			byte[] content = DBS.getBackupsFileManager().getChunkContent(chunkID);
			Chunk chunk = new Chunk(info.getChunkID(),content,info.getDesiredReplication());
			
			DBS.getDatabase().resetChunkReplication(chunkID);
			
			new BackupChunk(chunk).run();
			
			if (info.getOverReplication() > 0)
			{
				usedSpace -= deleteChunk(info);
			}
			else
			{
				DBS.getMessageBuilder().sendStored(chunkID.getFileId(), chunkID.getNumber());
			}
			
			int delay = random.nextInt(51); // [0,50]
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) { }
		}
		if (usedSpace <= backup_space)
			System.out.println("Space reclaimed successfully.");
		else
			System.out.println("Space could not be fully reclaimed due to low replication degrees.");
	}
	
	private int deleteChunk(ChunkInfo info)
	{
		ChunkID chunkID = info.getChunkID();
		File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
		file.delete();
		DBS.getDatabase().removeReceivedBackup(chunkID, false);
		return info.getSize();
	}
}