package handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;
import messages.ChunkID;

public class RemovedHandler extends Handler {
	
	//REMOVED <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile("REMOVED(?: )+([0-9]\\.[0-9])(?: )+([0-9]+)(?: )+(.{64})(?: )+([0-9]+)(?: )*.*?\r\n\r\n");
	
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
			DBS.getDatabase().removeChunkPeer(chunkID, sender);
			System.out.println("REMOVED handled succesfully");
		}
		else System.out.print("Invalid REMOVED received");
	}

}
