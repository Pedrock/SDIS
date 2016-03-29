
import java.io.IOException;
import java.rmi.activation.UnknownObjectException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import server.main.DBS;
import server.tasks.Backup;
import server.tasks.Delete;
import server.tasks.Restore;
import server.tasks.SpaceReclaiming;

class Server implements ServerInterface{
	
	private DBS dbs;
	
	Server(String[] args) throws IOException
	{
		Integer id = Integer.parseInt(args[0]);
		Integer mc_port = Integer.parseInt(args[2]);
		Integer mdb_port = Integer.parseInt(args[4]);
		Integer mdr_port = Integer.parseInt(args[6]);
		dbs = new DBS(id,args[1],mc_port,args[3],mdb_port,args[5],mdr_port);
	}
	
	private void start()
	{
		dbs.start();
	}
	
	public static void main(String[] args) {
		
		String remote_object_name = null;
		
		if (args.length == 7)
		{
			try {
				Server server = new Server(args);
				remote_object_name = args[0];
				ServerInterface stub = (ServerInterface) UnicastRemoteObject.exportObject(server,0);
				Registry registry = LocateRegistry.getRegistry();
				registry.bind(remote_object_name, stub);
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if (args.length == 0)
		{
			try {
				String[] default_args = {"1","224.0.0.0","4445","224.0.0.0","4446","224.0.0.0","4447"};
				Server server = new Server(default_args);
				remote_object_name = default_args[0];
				ServerInterface stub = (ServerInterface) UnicastRemoteObject.exportObject(server,0);
				Registry registry = LocateRegistry.getRegistry();
				registry.bind(remote_object_name, stub);
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else if (args.length == 1)
		{
			try {
				String[] default_args = {args[0],"224.0.0.0","4445","224.0.0.0","4446","224.0.0.0","4447"};
				Server server = new Server(default_args);
				remote_object_name = default_args[0];
				ServerInterface stub = (ServerInterface) UnicastRemoteObject.exportObject(server,0);
				Registry registry = LocateRegistry.getRegistry();
				registry.bind(remote_object_name, stub);
				server.start();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		else
		{
			 System.out.println("Invalid args");
			 return;
		}
		
		String remote_object_name2 = remote_object_name;
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				try {
					Registry registry = LocateRegistry.getRegistry();
					registry.unbind(remote_object_name2);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			}
		});
		
		System.out.println("Server started");
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
			throw new Exception("Unknown file");
		}
	}

	@Override
	public void delete(String filename) throws Exception {
		try {
			new Delete(filename).runWithExceptions();
		} catch (UnknownObjectException e) {
			throw new Exception("Unknown file");
		}
	}

	@Override
	public void spaceReclaiming(long new_space) throws Exception {
		new SpaceReclaiming(new_space).runWithExceptions();
	}
}
