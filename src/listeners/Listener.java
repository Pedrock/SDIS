package listeners;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;

import handlers.Handler;
import handlers.HandlerFactory;

public abstract class Listener implements Runnable{
	protected MulticastSocket socket;
	protected InetAddress address;
	protected byte[] buffer = new byte[256];
	private int port;
	
	public Listener(String address, int port) throws IOException {
		this.port = port;
		socket = new MulticastSocket(port);
		socket.setTimeToLive(1);
		this.address = InetAddress.getByName(address);
		socket.joinGroup(this.address);
	}
	
	public InetAddress getAddress()
	{
		return address;
	}
	
	public int getPort()
	{
		return port;
	}
	
	public MulticastSocket getSocket()
	{
		return socket;
	}
	
	@Override
	public void run() {
		while (true) {
            DatagramPacket msgPacket = new DatagramPacket(buffer, buffer.length);
            try {
				socket.receive(msgPacket);
				String msg = new String(msgPacket.getData(),msgPacket.getOffset(),msgPacket.getLength());
				System.out.println("Received: "+msg);
				Handler handler = HandlerFactory.getHandler(msg);
				if (handler != null)
				{
					System.out.println("Starting handler");
					new Thread(handler).start();
				}
            } catch (IOException e) {
				e.printStackTrace(); // TODO - remove print
			}
        }
	}
	
}
