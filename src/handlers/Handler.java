package handlers;

import java.util.Arrays;

public abstract class Handler implements Runnable {
	protected byte[] message;
	protected String header;
	
	public Handler(String header, byte[] message) {
		this.header = header;
		this.message = message;
	}
	
	public Handler(String header) {
		this.header = header;
	}
	
	protected byte[] getMessageBody()
	{
		return Arrays.copyOfRange(message, header.length(), message.length);
	}
}
