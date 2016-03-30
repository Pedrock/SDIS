package server.handlers;

import java.io.File;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;
import server.messages.ChunkID;
import server.tasks.Delete;

public class PutChunkHandler extends Handler {

	// PUTCHUNK <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF><CRLF><Body>
	final private static Pattern pattern = Pattern.compile(
			"PUTCHUNK(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
			Pattern.DOTALL);
	
	public PutChunkHandler(String header, byte[] message) {
		super(header,message);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			System.out.println("Valid PUTCHUNK received");
			
			String fileId = matcher.group(3);
			if (DBS.getDatabase().isMyFile(fileId)) return;
			
			Integer senderID = Integer.parseInt(matcher.group(2));
			Integer chunkNumber = Integer.parseInt(matcher.group(4));
			Integer replication = Integer.parseInt(matcher.group(5));
			ChunkID chunkID = new ChunkID(fileId, chunkNumber);
			
			if (DBS.getDatabase().isMyDeletedFile(fileId))
			{
				new Thread(new Delete(chunkID)).start();
				return;
			}
			
			byte[] content = getMessageBody();
			DBS.getMdbListener().handlePutChunk(senderID, fileId, chunkNumber, content);
			if (content == null)
			{
				System.out.println("Empty PUTCHUNK");
				return;
			}
			
			boolean backed_up = DBS.getDatabase().hasBackup(chunkID);
			long new_space_usage = DBS.getDatabase().getTotalUsedSpace() + content.length;
			
			if (!backed_up && new_space_usage > DBS.getDatabase().getBackupSpace())
			{
				System.out.println("Can not store. Not enought free space.");
				return;
			}
			
			int previous_replication = DBS.getDatabase().getChunkCurrentReplication(chunkID);
			
			DBS.getDatabase().resetChunkReplication(chunkID);
			
			Random random = new Random();
			int delay;
			
			if (!backed_up && previous_replication >= replication)
			{
				delay = 500 + random.nextInt(251); // [500,750]
			}
			else
			{
				delay = random.nextInt(401); // [0,400]
			}
			
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) { }
			
			int current_replication = DBS.getDatabase().getChunkCurrentReplication(chunkID);
			
			if (!backed_up && current_replication < replication)
			{
				File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
				DBS.getDatabase().addReceivedBackup(chunkID, content.length, replication);
				if (!file.exists())
				{
					DBS.getBackupsFileManager().createFile(file.getName(), content);
					System.out.println("Chunk stored");
				}
				backed_up = true;
			}
			if (backed_up)
				DBS.getMessageBuilder().sendStored(fileId,chunkNumber);
			else
				DBS.getDatabase().addChunkInfo(chunkID, content.length, replication);
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
