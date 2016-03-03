package handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;

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
			
			String filename = fileId+"-"+chunkNumber;
			boolean file_exists = DBS.getBackupsFileManager().fileExists(filename);
			if (file_exists)
			{
				byte[] content = DBS.getBackupsFileManager().getFileContents(filename);
				if (content != null)
				{
					DBS.getMessageBuilder().sendChunk(fileId,chunkNumber,content);
					System.out.println("CHUNK sent");
				}
			}
			System.out.println("GETCHUNK handled succesfully");
		}
		else System.out.print("Invalid GETCHUNK received");
	}

}
