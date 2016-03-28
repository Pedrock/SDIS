package server.main;
import java.io.IOException;

import server.filesystem.Database;
import server.filesystem.FileManager;
import server.listeners.McListener;
import server.listeners.MdbListener;
import server.listeners.MdrListener;
import server.messages.MessageBuilder;

public class DBS {
	
	private static final String PROTOCOL_VERSION = "1.0";
	public final static int CHUNK_SIZE = 64000;
	public final static int MAX_CHUNK_SIZE = 65536;
	
	private static int id = 1;
	
	private static volatile long backup_space = 3200000;
	
	private static McListener mcListener;
	private static MdbListener mdbListener;
	private static MdrListener mdrListener;
	
	private static FileManager localFM;
	private static FileManager backupsFM;
	private static FileManager restoredFM;
	
	private static MessageBuilder messageBuilder;
	
	private static Database database;
	
	DBS(String mc_addr,int mc_port,String mdb_addr,int mdb_port,String mdr_addr,int mdr_port) throws IOException {
		mcListener = new McListener(mc_addr, mc_port);
		mdbListener = new MdbListener(mdb_addr, mdb_port);
		mdrListener = new MdrListener(mdr_addr, mdr_port);
		localFM = new FileManager("Files");
		backupsFM = new FileManager("Backups");
		restoredFM = new FileManager("Restored");
		messageBuilder = new MessageBuilder();
		database = Database.fromFile();
		if (database == null)
			database = new Database();
	}
	
	public DBS(int id, String mc_addr,int mc_port,String mdb_addr,int mdb_port,String mdr_addr,int mdr_port) throws IOException {
		this(mc_addr,mc_port,mdb_addr,mdb_port,mdr_addr,mdr_port);
		DBS.id = id;
	}

	public void start() {
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
		return PROTOCOL_VERSION;
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
	
	public static FileManager getRestoredFileManager()
	{
		return restoredFM;
	}
	
	public static MessageBuilder getMessageBuilder()
	{
		return messageBuilder;
	}
	
	public static Database getDatabase()
	{
		return database;
	}

	public static long getBackupSpace() {
		return backup_space;
	}
	
	public static void setBackupSpace(long backup_space)
	{
		DBS.backup_space = backup_space;
	}
}