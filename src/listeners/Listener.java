package listeners;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.Arrays;

import handlers.Handler;
import handlers.HandlerFactory;
import main.DBS;

public abstract class Listener implements Runnable{
	
	protected MulticastSocket socket;
	protected InetAddress address;
	protected byte[] buffer = new byte[DBS.MAX_CHUNK_SIZE];
	protected int port;
	
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
	
	@Override
	public void run() {
		while (true) {
            DatagramPacket msgPacket = new DatagramPacket(buffer, buffer.length);
            try {
				socket.receive(msgPacket);
				byte[] msg = Arrays.copyOfRange(msgPacket.getData(),msgPacket.getOffset(),msgPacket.getOffset()+msgPacket.getLength());
				System.out.println("Packet Received: "+msg);
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

	public DatagramSocket getSocket() {
		return socket;
	}
	
}
