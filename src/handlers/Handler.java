package handlers;

public abstract class Handler implements Runnable {
	protected String message;
	
	public Handler(String message) {
		this.message = message;
		System.out.println("Handler created for "+message); // TODO - debug
	}
}
