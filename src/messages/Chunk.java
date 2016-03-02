package messages;

public class Chunk {
	private int number;
	private String fileId;
	private byte[] chunkData;
	private int replication;
	
	public Chunk(String fileId, int number)
	{
		this.number = number;
		this.fileId = fileId;
	}
	
	public Chunk(String fileId, int number, byte[] chunkData, int replication) {
		this.number = number;
		this.fileId = fileId;
		this.chunkData = chunkData;
		this.replication = replication;
	}

	public int getNumber()
	{
		return number;
	}
	
	public String getFileId() {
		return fileId;
	}

	public byte[] getChunkData() {
		return chunkData;
	}
	
	public int getReplicationDegree() {
		return replication;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fileId == null) ? 0 : fileId.hashCode());
		result = prime * result + number;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Chunk other = (Chunk) obj;
		if (fileId == null) {
			if (other.fileId != null)
				return false;
		} else if (!fileId.equals(other.fileId))
			return false;
		if (number != other.number)
			return false;
		return true;
	}
	
	
}
