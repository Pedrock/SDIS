package handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;

public class HandlerFactory {
	
	// <MessageType> <Version> <SenderId>
	final private static Pattern pattern = Pattern.compile(
			"^(\\S+)(?: )+([0-9].[0-9])(?: )+([0-9]+)(?: ).+?\r\n",
			Pattern.DOTALL);
	
	public static Handler getHandler(byte[] message)
	{
		Matcher matcher = pattern.matcher(new String(message));
		if (!matcher.find()) return null;
		String header = matcher.group(0);
		int senderId = Integer.parseInt(matcher.group(3));
		if (senderId == DBS.getId()) return null; // Ignore own requests
		switch (matcher.group(1)) // Message type
		{
			case "PUTCHUNK":
				return new PutChunkHandler(header,message);
			case "STORED":
				return new StoredHandler(header,message);
			default:
				// TODO
				return null;
		}
	}
}
