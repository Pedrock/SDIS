package server.handlers;

import java.io.File;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.filesystem.DatabaseManager;
import server.main.DBS;
import server.messages.ChunkID;

public class DeleteHandler extends Handler {
	
	//DELETE <Version> <SenderId> <FileId> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile("DELETE(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )*.*?\r\n\r\n");
	
	public DeleteHandler(String header) {
		super(header);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			String fileId = matcher.group(3);
			DatabaseManager db = DBS.getDatabase();
			Set<Integer> chunksSet = db.getFileChunks(fileId);
			if (chunksSet != null) {
				Integer[] chunks = chunksSet.toArray(new Integer[chunksSet.size()]);
				if (chunks != null)
				{
					for (Integer chunk : chunks) 
					{
						ChunkID chunkId = new ChunkID(fileId, chunk);
						File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
						file.delete();
						db.removeReceivedBackup(chunkId, true);
					}
				}
			}
		}
		else System.out.print("Invalid DELETE received");
	}

}
