package handlers;

import java.io.File;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;
import messages.ChunkID;

public class PutChunkHandler extends Handler {

	// PUTCHUNK <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF><CRLF><Body>
	final private static Pattern pattern = Pattern.compile(
			"PUTCHUNK(?: )+([0-9]\\.[0-9])(?: )+([0-9]+)(?: )+(.{64})(?: )+([0-9]+)(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
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
			Integer chunkNumber = Integer.parseInt(matcher.group(4));
			Integer replication = Integer.parseInt(matcher.group(5));
			ChunkID chunkId = new ChunkID(fileId, chunkNumber);
			byte[] content = getMessageBody();
			if (content == null)
			{
				System.out.println("Empty PUTCHUNK");
				return;
			}
			
			if (!DBS.getDatabase().hasBackup(chunkId))
			{
				if (DBS.getDatabase().getTotalUsedSpace()+content.length > DBS.getBackupSpace())
				{
					System.out.println("Can not store. Not enought free space.");
					return;
				}
				File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
				DBS.getDatabase().addReceivedBackup(chunkId, content.length, replication);
				if (!file.exists())
				{
					DBS.getBackupsFileManager().createFile(file.getName(), content);
					System.out.println("Chunk stored");
				}
			}
			Random random = new Random();
			int delay = random.nextInt(401); // [0,400]
			try {
				Thread.sleep(delay);
			} catch (InterruptedException e) { }
			DBS.getMessageBuilder().sendStored(fileId,chunkNumber);
			
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
