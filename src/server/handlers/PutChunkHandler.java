package server.handlers;

import java.io.File;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;
import server.messages.ChunkID;

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
			ChunkID chunkId = new ChunkID(fileId, chunkNumber);
			
			byte[] content = getMessageBody();
			DBS.getMdbListener().handlePutChunk(senderID, fileId, chunkNumber, content);
			if (content == null)
			{
				System.out.println("Empty PUTCHUNK");
				return;
			}
			
			boolean backed_up = DBS.getDatabase().hasBackup(chunkId);
			long new_space_usage = DBS.getDatabase().getTotalUsedSpace() + content.length;
			
			if (!backed_up && new_space_usage > DBS.getBackupSpace())
			{
				System.out.println("Can not store. Not enought free space.");
				return;
			}
			
			Random random = new Random();
			int delay = random.nextInt(401); // [0,400]
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) { }
			
			int current_replication = DBS.getDatabase().getChunkCurrentReplication(chunkId);
			
			if (!backed_up && current_replication < replication)
			{
				File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
				DBS.getDatabase().addReceivedBackup(chunkId, content.length, replication);
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
				DBS.getDatabase().addChunkInfo(chunkId, content.length, replication);
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
