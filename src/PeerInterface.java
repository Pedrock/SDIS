

import java.rmi.Remote;

interface PeerInterface extends Remote {
	public void backup(String filename, int replication) throws Exception;
	public void restore(String filename) throws Exception;
	public void delete(String filename) throws Exception;
	public void spaceReclaiming(long new_space) throws Exception;
}
