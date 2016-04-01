package server.handlers;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;

public class HandlerFactory {
	
	// <MessageType> <Version> <SenderId>
	final private static Pattern pattern = Pattern.compile(
			"^(\\S+)(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: ).+?\r\n\r\n",
			Pattern.DOTALL);
	
	public static Handler getHandler(byte[] message, InetAddress address)
	{
		Matcher matcher = pattern.matcher(new String(message,StandardCharsets.US_ASCII));
		if (!matcher.find()) return null;
		String header = matcher.group(0);
		String version = matcher.group(2);
		int senderId = Integer.parseInt(matcher.group(3));
		if (senderId == DBS.getId()) return null; // Ignore own requests
		System.out.println("Handler for "+matcher.group(1));
		if (!version.equals(DBS.getProtocolVersion()))
		{
			System.out.println("Message ignored due to different protocol version.");
			return null;
		}
		switch (matcher.group(1)) // Message type
		{
			case "PUTCHUNK":
				return new PutChunkHandler(header,message);
			case "STORED":
				return new StoredHandler(header);
			case "GETCHUNK":
				return new GetChunkHandler(header,address);
			case "CHUNK":
				return new ChunkHandler(header,message);
			case "DELETE":
				return new DeleteHandler(header);
			case "REMOVED":
				return new RemovedHandler(header);
			default:
				System.out.println("Handler unavailable");
				return null;
		}
	}
}
