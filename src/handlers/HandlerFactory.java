package handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import main.DBS;

public class HandlerFactory {
	
	// <MessageType> <Version> <SenderId>
	final private static Pattern pattern = Pattern.compile(
			"(\\S+)(?: )+([0-9].[0-9])(?: )+([0-9]+)(?: ).+",
			Pattern.DOTALL);
	
	public static Handler getHandler(String message)
	{
		Matcher matcher = pattern.matcher(message);
		if (!matcher.matches()) return null;
		int senderId = Integer.parseInt(matcher.group(3));
		if (senderId == DBS.getId()) return null; // Ignore own requests
		switch (matcher.group(1)) // Message type
		{
			case "PUTCHUNK":
				return new PutChunkHandler(message);
			case "STORED":
				return new StoredHandler(message);
			default:
				// TODO
				return null;
		}
	}
}
