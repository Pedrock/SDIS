package server.tasks;

import java.io.File;
import java.util.SortedSet;

import server.filesystem.ChunkInfo;
import server.main.DBS;
import server.main.PeerError;
import server.messages.Chunk;
import server.messages.ChunkID;

public class SpaceReclaiming implements Runnable {
	
	private static final int SLEEP = 1000;
	
	private long backup_space;
	
	public SpaceReclaiming(long backup_space) {
		this.backup_space = backup_space;
	}
	
	public void runWithExceptions() throws Exception
	{
		DBS.getDatabase().setBackupSpace(backup_space);
		SortedSet<ChunkInfo> infos = DBS.getDatabase().getBackupChunksInfo();
		long usedSpace = DBS.getDatabase().getTotalUsedSpace();
		for (ChunkInfo info : infos)
		{
			if (usedSpace <= backup_space) break; // Objective achieved
			ChunkID chunkID = info.getChunkID();
			
			DBS.getDatabase().removeReceivedBackup(chunkID, false);
			DBS.getDatabase().resetChunkReplication(chunkID);
			
			DBS.getMessageBuilder().sendRemoved(chunkID.getFileId(),chunkID.getNumber());
			
			try {
				Thread.sleep(SLEEP);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			if (!DBS.isRunning()) throw new PeerError("Server stopped");
			
			int replication = info.getDesiredReplication();
			
			byte[] content = DBS.getBackupsFileManager().getChunkContent(chunkID);
			
			DBS.getDatabase().addReceivedBackup(chunkID, info.getSize(), info.getDesiredReplication());
			
			if (content != null && (DBS.getDatabase().getChunkCurrentReplication(chunkID)-1) < replication)
			{
				Chunk chunk = new Chunk(info.getChunkID(),content,info.getDesiredReplication());
				BackupChunk task = new BackupChunk(chunk,true);
				task.run();
			}
			
			if (content == null || DBS.getDatabase().getChunkCurrentReplication(chunkID) > replication)
			{
				usedSpace -= deleteChunk(info);
			}
			else
			{
				DBS.getMessageBuilder().sendStored(chunkID.getFileId(), chunkID.getNumber());
			}
			if (!DBS.isRunning()) throw new PeerError("Server stopped");
		}
		if (usedSpace <= backup_space)
			System.out.println("Space reclaimed successfully.");
		else
			throw new PeerError("Space could not be fully reclaimed due to low replication degrees.");
	}
	
	@Override
	public void run() {
		try 
		{
			runWithExceptions();
		}
		catch (Exception ex)
		{
			System.out.println(ex.getMessage());
		}
	}
	
	private int deleteChunk(ChunkInfo info)
	{
		ChunkID chunkID = info.getChunkID();
		File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
		if (file.exists()) file.delete();
		DBS.getDatabase().removeReceivedBackup(chunkID, false);
		return info.getSize();
	}
}