package server.handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;

public class StoredHandler extends Handler {
	
	// STORED <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile("STORED(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )*.*?\r\n\r\n");
	
	public StoredHandler(String header) {
		super(header);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			String sender = matcher.group(2);
			String fileId = matcher.group(3);
			int chunkNumber = Integer.parseInt(matcher.group(4));
			DBS.getMcListener().handleStored(sender,fileId,chunkNumber);
		}
		else System.out.print("Invalid STORED received");
	}

}
