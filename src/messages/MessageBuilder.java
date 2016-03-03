package messages;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;

import listeners.Listener;
import main.DBS;

public class MessageBuilder {
	
	MulticastSocket socket;
	
	public MessageBuilder() throws IOException {
		socket = new MulticastSocket();
	}
	
	public void sendPutChunk(Chunk chunk)
	{
		byte[] header = buildHeader(
				"PUTCHUNK",
				chunk.getFileId(),
				chunk.getNumber(),
				chunk.getReplicationDegree());
		byte[] body = chunk.getChunkData();
		
		byte[] message = new byte[header.length + body.length];
		System.arraycopy(header,0,message,0            ,header.length);
		System.arraycopy(body,  0,message,header.length,body.length);
		
		sendToMdb(message);
	}
	
	public void sendStored(String fileId, String chunkNumber) {
		byte[] message = buildHeader(
				"STORED",
				fileId,
				chunkNumber);
		sendToMc(message);
	}
	
	public void sendChunk(String fileId, int chunkNumber, byte[] body)
	{
		byte[] header = buildHeader(
				"CHUNK",
				fileId,
				chunkNumber);
		
		byte[] message = new byte[header.length + body.length];
		System.arraycopy(header,0,message,0            ,header.length);
		System.arraycopy(body,  0,message,header.length,body.length);
		
		sendToMdr(message);
	}
	
	public void sendGetChunk(String fileId, int chunkNumber) {
		byte[] message = buildHeader(
				"GETCHUNK",
				fileId,
				chunkNumber);
		sendToMc(message);
	}
	
	private byte[] buildHeader(String messageType, Object... args)
	{
		StringBuilder sb = new StringBuilder();
		sb.append(messageType); sb.append(' ');
		sb.append(DBS.getProtocolVersion()); sb.append(' ');
		sb.append(DBS.getId()); sb.append(' ');
		for (int i = 0; i < args.length; i++)
		{
			sb.append(args[i]);
			sb.append(' ');
		}
		sb.append("\r\n\r\n");
		System.out.println("Sending "+messageType);
		return sb.toString().getBytes(StandardCharsets.US_ASCII);
	}
	
	private void sendToMc(byte[] message)
	{
		sendPacket(DBS.getMcListener(), message);
	}
	
	private void sendToMdb(byte[] message)
	{
		sendPacket(DBS.getMdbListener(), message);
	}
	
	private void sendToMdr(byte[] message)
	{
		sendPacket(DBS.getMdrListener(), message);
	}
	
	private synchronized void sendPacket(Listener listener, byte[] message)
	{
		InetAddress address = listener.getAddress();
		int port = listener.getPort();
		DatagramPacket packet = new DatagramPacket(message,message.length,address,port);
		try {
			socket.send(packet);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
