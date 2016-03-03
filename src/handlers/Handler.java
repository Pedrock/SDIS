package handlers;

public abstract class Handler implements Runnable {
	protected byte[] message;
	protected String header;
	
	public Handler(String header, byte[] message) {
		this.header = header;
		this.message = message;
	}
}
