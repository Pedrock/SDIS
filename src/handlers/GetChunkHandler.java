package handlers;

import java.io.File;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;
import messages.ChunkID;

public class GetChunkHandler extends Handler {

	// GETCHUNK <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile(
			"GETCHUNK(?: )+([0-9]\\.[0-9])(?: )+([0-9]+)(?: )+(.{64})(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
			Pattern.DOTALL);
	
	public GetChunkHandler(String header) {
		super(header);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			System.out.println("Valid GETCHUNK received");
			//int sender = Integer.parseInt(matcher.group(2));
			String fileId = matcher.group(3);
			int chunkNumber = Integer.parseInt(matcher.group(4));
			ChunkID chunkId = new ChunkID(fileId, chunkNumber);
			File file = DBS.getBackupsFileManager().getFile(chunkId.toString());
			if (file.exists())
			{
				byte[] content = DBS.getBackupsFileManager().getFileContents(file.getName());
				if (content != null)
				{
					Random random = new Random();
					int delay = random.nextInt(401); // [0,400]
					
					DBS.getMdrListener().notifyOnChunk(this, chunkId);
					
					synchronized(this) {
						try {
							wait(delay);
						} catch (InterruptedException e) {}
					}
					
					if (DBS.getMdrListener().getChunk(chunkId) == null)
					{
						DBS.getMessageBuilder().sendChunk(fileId,chunkNumber,content);
						System.out.println("CHUNK sent");
					}
					else
					{
						System.out.println("CHUNK not sent");
					}
					
				}
			}
			System.out.println("GETCHUNK handled succesfully");
		}
		else System.out.print("Invalid GETCHUNK received");
	}

}
