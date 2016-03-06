package messages;

public class ChunkID
{
	private int number;
	private String fileId;
	
	public ChunkID(String fileId, int number) {
		this.number = number;
		this.fileId = fileId;
	}
	
	
	
	@Override
	public String toString() {
		return fileId + "-" + number;
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
		ChunkID other = (ChunkID) obj;
		if (fileId == null) {
			if (other.fileId != null)
				return false;
		} else if (!fileId.equals(other.fileId))
			return false;
		if (number != other.number)
			return false;
		return true;
	}

	public int getNumber() {
		return number;
	}

	public String getFileId() {
		return fileId;
	}
}
