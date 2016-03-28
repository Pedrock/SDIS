package server.handlers;

import java.io.File;
import java.net.InetAddress;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;
import server.messages.ChunkID;

public class GetChunkHandler extends Handler {
	// GETCHUNK <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile(
			"GETCHUNK(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
			Pattern.DOTALL);
	
	private InetAddress address;
	private int port;
	
	public GetChunkHandler(String header, InetAddress address, int port) {
		super(header);
		this.address = address;
		this.port = port;
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
			ChunkID chunkID = new ChunkID(fileId, chunkNumber);
			File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
			if (file.exists())
			{
				byte[] content = DBS.getBackupsFileManager().getFileContents(file.getName());
				if (content != null)
				{
					Random random = new Random();
					int delay = random.nextInt(401); // [0,400]
					
					DBS.getMdrListener().notifyOnChunk(this, chunkID);
					
					synchronized(this) {
						try {
							wait(delay);
						} catch (InterruptedException e) {}
					}
					
					int chance = DBS.getDatabase().getChunkCurrentReplication(chunkID);
					if (chance == 0) chance = 1;
					
					if (DBS.getMdrListener().getChunk(chunkID) == null)
					{
						if (random.nextInt(chance) == 0)
						{
							DBS.getMessageBuilder().sendChunk(fileId,chunkNumber,content,address,port);
							System.out.println("CHUNK sent");
						}
						else
							System.out.println("CHUNK not sent because of the probability");
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
