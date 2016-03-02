package handlers;

import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;

public class PutChunkHandler extends Handler {

	// PUTCHUNK <Version> <SenderId> <FileId> <ChunkNo> <ReplicationDeg> <CRLF><CRLF><Body>
	final private static Pattern pattern = Pattern.compile(
			"PUTCHUNK(?: )+([0-9].[0-9])(?: )+([0-9]+)(?: )+(.{64})(?: )+([0-9]+)(?: )+([0-9]+)(?: )+\r\n\r\n(.+)",
			Pattern.DOTALL);
	
	public PutChunkHandler(String message) {
		super(message);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(message);
		if (matcher.matches())
		{
			System.out.println("Valid PUTCHUNK received");
			String fileId = matcher.group(3);
			String chunkNumber = matcher.group(4);
			String filename = fileId+"-"+chunkNumber;
			boolean file_exists = DBS.getBackupsFileManager().fileExists(filename);
			if (!file_exists)
			{
				byte[] content = matcher.group(6).getBytes(StandardCharsets.US_ASCII);
				DBS.getBackupsFileManager().createFile(filename, content);
				System.out.println("Chunk stored");
			}
			DBS.getMessageBuilder().sendStored(fileId,chunkNumber);
		}
		else System.out.println("Invalid PUTCHUNK received");
	}

}
