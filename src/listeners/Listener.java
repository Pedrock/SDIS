package listeners;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;

import handlers.Handler;
import handlers.HandlerFactory;
import main.DBS;

public abstract class Listener implements Runnable{
	protected MulticastSocket socket;
	protected InetAddress address;
	protected byte[] buffer = new byte[2*DBS.CHUNK_SIZE];
	private int port;
	
	public Listener(String address, int port) throws IOException {
		this.port = port;
		socket = new MulticastSocket(port);
		socket.setTimeToLive(1);
		this.address = InetAddress.getByName(address);
		socket.joinGroup(this.address);
	}
	
	public synchronized InetAddress getAddress()
	{
		return address;
	}
	
	public synchronized int getPort()
	{
		return port;
	}
	
	public synchronized MulticastSocket getSocket()
	{
		return socket;
	}
	
	@Override
	public void run() {
		while (true) {
            DatagramPacket msgPacket = new DatagramPacket(buffer, buffer.length);
            try {
				socket.receive(msgPacket);
				byte[] msg = Arrays.copyOfRange(msgPacket.getData(),msgPacket.getOffset(),msgPacket.getOffset()+msgPacket.getLength());
				System.out.println("Packet Received");
				Handler handler = HandlerFactory.getHandler(msg);
				if (handler != null)
				{
					new Thread(handler).start();
				}
            } catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
}
