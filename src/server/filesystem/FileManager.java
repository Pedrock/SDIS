package server.filesystem;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import server.messages.ChunkID;

public class FileManager {
	private Path path;
	private MessageDigest md;
	
	public FileManager(Path path) throws IOException {
		this.path = path;
		try {
			md = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return;
		}
		if (Files.exists(path))
		{
			if (!Files.isDirectory(path))
				throw new FileNotFoundException(path.toString());
		}
		else path.toFile().mkdirs();
	}
	
	public boolean fileExists(String file)
	{
		return path.resolve(file).toFile().exists(); 
	}
	
	public File getFile(String filename)
	{
		return path.resolve(filename).toFile();
	}
	
	public byte[] getFileContents(String filename)
	{
		try {
			return Files.readAllBytes(getFile(filename).toPath());
		} catch (IOException e) {
			return null;
		}
	}
	
	public byte[] getChunkContent(ChunkID chunkID)
	{
		return getFileContents(chunkID.toString());
	}
	
	public boolean createFile(String filename, byte[] content)
	{
		try {
			Path file = path.resolve(filename);
			Files.write(file,content);
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public String generateFileHash(String filename) throws FileNotFoundException
	{
		if (!fileExists(filename)) 
			throw new FileNotFoundException(filename);
		File file = path.resolve(filename).toFile();
		String bitstring = file.getName()+file.lastModified()+file.length();
		byte[] hash = md.digest(bitstring.getBytes(StandardCharsets.UTF_8));
		StringBuffer sb = new StringBuffer();
		
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(hash[i] & 0xff);
            if (hex.length() < 2) sb.append('0');
            sb.append(hex);
        }
        
		return sb.toString();
	}
}
