package handlers;

import java.io.File;
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
			ChunkID chunkId = new ChunkID(fileId, chunkNumber);
			File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
			if (!file.exists())
			{
				byte[] content = getMessageBody();
				if (content != null)
				{
					DBS.getBackupsFileManager().createFile(file.getName(), content);
					DBS.getDatabase().addReceivedBackup(chunkId);
					System.out.println("Chunk stored");
				}
			}
			DBS.getMessageBuilder().sendStored(fileId,chunkNumber);
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
