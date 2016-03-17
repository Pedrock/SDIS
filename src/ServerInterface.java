

import java.rmi.Remote;
import java.rmi.RemoteException;

interface ServerInterface extends Remote {
	public void backup(String filename, int replication) throws RemoteException;
	public void restore(String filename) throws RemoteException;
	public void delete(String filename) throws RemoteException;
	public void spaceReclaiming(long new_space) throws RemoteException;
}
