package server.main;

public class PeerError extends Exception {
	private static final long serialVersionUID = 3015748924676995L;

	public PeerError(String message) {
		super(message);
	}
}
