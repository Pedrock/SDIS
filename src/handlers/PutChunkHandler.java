package handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;

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
			String chunkNumber = matcher.group(4);
			String filename = fileId+"-"+chunkNumber;
			boolean file_exists = DBS.getBackupsFileManager().fileExists(filename);
			if (!file_exists)
			{
				byte[] content = getMessageBody();
				if (content != null)
				{
					DBS.getBackupsFileManager().createFile(filename, content);
					System.out.println("Chunk stored");
				}
			}
			DBS.getMessageBuilder().sendStored(fileId,chunkNumber);
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
