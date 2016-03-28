package server.filesystem;

import java.io.Serializable;
import java.util.HashSet;

import server.messages.ChunkID;

public class ChunkInfo implements Comparable<ChunkInfo>, Serializable {
	private static final long serialVersionUID = 3925311055955677982L;
	
	private ChunkID chunkID = null;
	private Integer size;
	private Integer replication;
	private HashSet<Integer> peers;
	
	public ChunkInfo() {
		this.peers = new HashSet<Integer>();
	}
	
	public ChunkInfo(Integer size, Integer replication) {
		this.size = size;
		this.replication = replication;
		this.peers = new HashSet<Integer>();
	}
	
	public ChunkInfo(ChunkID chunkID, ChunkInfo chunkInfo) {
		this.chunkID = chunkID;
		this.size = chunkInfo.size;
		this.replication = chunkInfo.replication;
		this.peers = chunkInfo.peers;
	}
	
	public void setInfo(Integer size, Integer replication)
	{
		this.size = size;
		this.replication = replication;
	}
	
	public ChunkID getChunkID() {
		return chunkID;
	}

	public Integer getSize() {
		return size;
	}

	public Integer getDesiredReplication() {
		return replication;
	}
	
	public HashSet<Integer> getPeers()
	{
		return peers;
	}
	
	public int getActualReplication()
	{
		return peers.size();
	}
	
	public int getOverReplication()
	{
		// TODO - if (replication == null) -1;
		return peers.size() - replication;
	}

	@Override
	public int compareTo(ChunkInfo other) {
		if (this.getOverReplication() < other.getOverReplication()) return 1;
		if (this.getOverReplication() > other.getOverReplication()) return -1;
		if (this.size < other.size) return 1;
		if (this.size > other.size) return -1;
		return -1; //Never equal
	}

	@Override
	public String toString() {
		return "ChunkInfo [chunkID=" + chunkID + ", size=" + size + ", replication=" + replication + ", peers=" + peers
				+ "]";
	}

	@Override
	public int hashCode() {
		if (chunkID != null) return chunkID.hashCode();
		final int prime = 31;
		int result = 1;
		result = prime * result + ((peers == null) ? 0 : peers.hashCode());
		result = prime * result + ((replication == null) ? 0 : replication.hashCode());
		result = prime * result + ((size == null) ? 0 : size.hashCode());
		return result;
	}

	public void resetReplication() {
		peers.clear();
	}
}
