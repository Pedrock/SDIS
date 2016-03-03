package main;
import java.io.IOException;

import filesystem.FileManager;
import listeners.McListener;
import listeners.MdbListener;
import listeners.MdrListener;
import messages.MessageBuilder;

public class DBS {
	
	private static final String protocolVersion = "1.0";
	public final static int CHUNK_SIZE = 64000;
	
	private static int id = 1;
	
	private static McListener mcListener;
	private static MdbListener mdbListener;
	private static MdrListener mdrListener;
	
	private static FileManager localFM;
	private static FileManager backupsFM;
	
	private static MessageBuilder messageBuilder;
	
	DBS(String mc_addr,int mc_port,String mdb_addr,int mdb_port,String mdr_addr,int mdr_port) throws IOException {
		mcListener = new McListener(mc_addr, mc_port);
		mdbListener = new MdbListener(mdb_addr, mdb_port);
		mdrListener = new MdrListener(mdr_addr, mdr_port);
		localFM = new FileManager("Files");
		backupsFM = new FileManager("Backups");
		messageBuilder = new MessageBuilder();
	}
	
	DBS(int id, String mc_addr,int mc_port,String mdb_addr,int mdb_port,String mdr_addr,int mdr_port) throws IOException {
		this(mc_addr,mc_port,mdb_addr,mdb_port,mdr_addr,mdr_port);
		DBS.id = id;
	}

	void run() {
		new Thread(mcListener).start();
		new Thread(mdbListener).start();
		new Thread(mdrListener).start();
	}
	
	public static int getId()
	{
		return id;
	}
	
	public static String getProtocolVersion()
	{
		return protocolVersion;
	}
	
	public static McListener getMcListener()
	{
		return mcListener;
	}
	
	public static MdbListener getMdbListener()
	{
		return mdbListener;
	}
	
	public static MdrListener getMdrListener()
	{
		return mdrListener;
	}
	
	public static FileManager getLocalFileManager()
	{
		return localFM;
	}
	
	public static FileManager getBackupsFileManager()
	{
		return backupsFM;
	}
	
	public static MessageBuilder getMessageBuilder()
	{
		return messageBuilder;
	}
	
}
