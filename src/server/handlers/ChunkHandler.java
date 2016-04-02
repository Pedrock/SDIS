package server.handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;

public class ChunkHandler extends Handler {

	// CHUNK <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF><Body>
	final private static Pattern pattern = Pattern.compile(
			"CHUNK(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
			Pattern.DOTALL);

	public ChunkHandler(String header, byte[] message) {
		super(header, message);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			String fileId = matcher.group(3);
			int chunkNumber = Integer.parseInt(matcher.group(4));
			DBS.getMdrListener().handleChunk(fileId, chunkNumber, getMessageBody());
		}
	}

}
