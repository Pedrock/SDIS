package server.handlers;

import java.io.File;
import java.net.InetAddress;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;
import server.messages.Chunk;
import server.messages.ChunkID;

public class GetChunkHandler extends Handler {
	// GETCHUNK <Version> <SenderId> <FileId> <ChunkNo> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile(
			"GETCHUNK(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )+([0-9]+)(?: )+.*?\r\n\r\n",
			Pattern.DOTALL);
	
	private InetAddress address;
	private Random random = new Random();
	
	public GetChunkHandler(String header, InetAddress address) {
		super(header);
		this.address = address;
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			String fileId = matcher.group(3);
			int chunkNumber = Integer.parseInt(matcher.group(4));
			ChunkID chunkID = new ChunkID(fileId, chunkNumber);
			File file = DBS.getBackupsFileManager().getFile(chunkID.toString());
			if (file.exists())
			{
				byte[] content = DBS.getBackupsFileManager().getFileContents(file.getName());
				if (content != null)
				{
					int delay = random.nextInt(401); // [0,400]
					
					DBS.getMdrListener().notifyOnChunk(this, chunkID);
					
					synchronized(this) {
						try {
							wait(delay);
						} catch (InterruptedException e) {}
					}
					
					int chance = DBS.getDatabase().getChunkCurrentReplication(chunkID);
					boolean multicast = DBS.getDatabase().getchunkReceived(chunkID);
					
					Chunk chunk = DBS.getMdrListener().getChunk(chunkID);
					DBS.getMdrListener().stopListenToChunk(chunkID);
					
					if (chunk == null)
					{
						if (multicast)
						{
							DBS.getMessageBuilder().sendChunk(fileId, chunkNumber, content);
							System.out.println("CHUNK sent via multicast");
						}
						else
						{
							if (chance == 0) chance = 1;
							if (random.nextInt(chance) == 0)
							{
								DBS.getMessageBuilder().sendChunk(fileId,chunkNumber,content,address);
								System.out.println("CHUNK sent via unicast");
							}
							else
							{
								System.out.println("CHUNK not sent because of the probability");
							}
						}
					}
					else
					{
						System.out.println("CHUNK not sent");
					}
					
				}
			}
		}
		else System.out.print("Invalid GETCHUNK received");
	}

}
