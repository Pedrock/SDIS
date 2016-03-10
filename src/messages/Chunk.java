package messages;

public class Chunk {
	private ChunkID id;
	private byte[] chunkData;
	private int replication;
	
	
	public Chunk(String fileId, int number, byte[] chunkData, int replication) {
		this.id = new ChunkID(fileId, number);
		this.chunkData = chunkData;
		this.replication = replication;
	}
	
	public Chunk(ChunkID chunkID, byte[] chunkData, int replication) {
		this.id = chunkID;
		this.chunkData = chunkData;
		this.replication = replication;
	}

	public int getNumber()
	{
		return id.getNumber();
	}
	
	public String getFileId() {
		return id.getFileId();
	}
	
	public ChunkID getID()
	{
		return id;
	}

	public byte[] getChunkData() {
		return chunkData;
	}
	
	public int getReplicationDegree() {
		return replication;
	}

	
	
	
}
