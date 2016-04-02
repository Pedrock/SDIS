
import java.io.IOException;
import java.rmi.activation.UnknownObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import server.main.DBS;
import server.main.PeerError;
import server.tasks.Backup;
import server.tasks.Delete;
import server.tasks.Restore;
import server.tasks.SpaceReclaiming;

class Peer implements PeerInterface{
	
	private DBS dbs;
	
	Peer(String[] args) throws IOException
	{
		String id = args[0];
		Integer mc_port = Integer.parseInt(args[2]);
		Integer mdb_port = Integer.parseInt(args[4]);
		Integer mdr_port = Integer.parseInt(args[6]);
		dbs = new DBS(id,args[1],mc_port,args[3],mdb_port,args[5],mdr_port);
		if (args.length == 8)
		{
			DBS.getDatabase().setBackupSpace(Long.parseLong(args[7]));
		}
	}
	
	private void start()
	{
		dbs.start();
		System.out.println("Server started");
	}
	
	public static void main(String[] args) {
		
		String remote_object_name = null;
		
		if (args.length == 7 || args.length == 8)
		{
			try {
				Peer server = new Peer(args);
				remote_object_name = args[0];
				PeerInterface stub = (PeerInterface) UnicastRemoteObject.exportObject(server,0);
				Registry registry = LocateRegistry.getRegistry();
				registry.bind(remote_object_name, stub);
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else
		{
			 System.out.println("Usage: java Peer <id> <MC_address> <MC_port> <MDB_address> <MDB_port> <MDR_address> <MDR_port> [allocated_backup_space]");
			 return;
		}
		
		final String remote_object_name2 = remote_object_name;
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					Registry registry = LocateRegistry.getRegistry();
					registry.unbind(remote_object_name2);
					System.out.println("RMI object unbinded");
				} catch (Exception e) {
				}
				System.out.println("Please wait...");
				DBS.stop();
				System.out.println("Server closed");
			}
		});
	}

	@Override
	public void backup(String filename, int replication) throws Exception {
		new Backup(filename,replication).runWithExceptions();
	}

	@Override
	public void restore(String filename) throws Exception {
		try {
			new Restore(filename).runWithExceptions();
		} catch (UnknownObjectException e) {
			throw new PeerError("Unknown file");
		}
	}

	@Override
	public void delete(String filename) throws Exception {
		try {
			new Delete(filename).runWithExceptions();
		} catch (UnknownObjectException e) {
			throw new PeerError("Unknown file");
		}
	}

	@Override
	public void spaceReclaiming(long new_space) throws Exception {
		new SpaceReclaiming(new_space).runWithExceptions();
	}
}
