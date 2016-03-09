package tasks;

import java.io.File;
import java.util.Random;
import java.util.SortedSet;

import filesystem.ChunkInfo;
import main.DBS;
import messages.ChunkID;

public class SpaceReclaiming implements Runnable {
	
	long backup_space;
	
	public SpaceReclaiming(long backup_space) {
		this.backup_space = backup_space;
	}
	
	
	public void run() {
		DBS.setBackupSpace(backup_space);
		SortedSet<ChunkInfo> infos = DBS.getDatabase().getSortedChunksInfo();
		long usedSpace = DBS.getDatabase().getTotalUsedSpace();
		System.out.println(usedSpace);
		for (ChunkInfo info : infos)
		{
			if (usedSpace <= backup_space) break;
			if (info.getOverReplication() > 0)
			{
				ChunkID chunkID = info.getChunkID();
				File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
				file.delete();
				DBS.getDatabase().removeReceivedBackup(chunkID, false);
				usedSpace -= info.getSize();
				
				Random random = new Random();
				int delay = random.nextInt(201); // [0,200]
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) { }
				DBS.getMessageBuilder().sendRemoved(chunkID.getFileId(),chunkID.getNumber());
			}
			else
			{
				System.out.println("Could not complete space reclaiming, not enough replication.");
				return;
			}
		}
		System.out.println("Space reclaimed successfully.");
	}
}