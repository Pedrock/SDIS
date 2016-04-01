package server.handlers;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.filesystem.DatabaseManager;
import server.main.DBS;
import server.messages.Chunk;
import server.messages.ChunkID;
import server.tasks.BackupChunk;
import server.tasks.Delete;

public class RemovedHandler extends Handler {
	
	//REMOVED <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile("REMOVED(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )*.*?\r\n\r\n");
	
	public RemovedHandler(String header) {
		super(header);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			int sender = Integer.parseInt(matcher.group(2));
			String fileId = matcher.group(3);
			Integer chunkNumber = Integer.parseInt(matcher.group(4));
			ChunkID chunkID = new ChunkID(fileId,chunkNumber);
			
			DatabaseManager db = DBS.getDatabase();
			
			if (db.isMyDeletedFile(fileId))
			{
				new Thread(new Delete(chunkID)).start();
				return;
			}
			
			db.removeChunkPeer(chunkID, sender);
			
			if (db.hasBackup(chunkID))
			{
				int current_replication = db.getChunkCurrentReplication(chunkID);
				Integer desired_replication = db.getChunkDesiredReplication(chunkID);
				
				if (current_replication < desired_replication)
				{
					DBS.getMdbListener().notifyOnPutChunk(this, chunkID);
					Random random = new Random();
					int delay = random.nextInt(501); // [0,500]
					synchronized(this) {
						try {
							wait(delay);
						} catch (InterruptedException e) {}
					}
					if (!DBS.getMdbListener().putChunkReceived(chunkID))
					{
						byte[] data = DBS.getBackupsFileManager().getChunkContent(chunkID);
						new BackupChunk(new Chunk(fileId, chunkNumber, data, desired_replication),true).run();
					}
				}
			}
			System.out.println("REMOVED handled succesfully");
		}
		else System.out.print("Invalid REMOVED received");
	}

}
