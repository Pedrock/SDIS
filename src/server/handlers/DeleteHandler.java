package server.handlers;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import server.main.DBS;

public class DeleteHandler extends Handler {
	
	//DELETE <Version> <SenderId> <FileId> <CRLF><CRLF>
	final private static Pattern pattern = Pattern.compile("DELETE(?: )+([0-9]\\.[0-9])(?: )+(\\S+)(?: )+(.{64})(?: )*.*?\r\n\r\n");
	
	public DeleteHandler(String header) {
		super(header);
	}

	@Override
	public void run() {
		Matcher matcher = pattern.matcher(header);
		if (matcher.matches())
		{
			int sender = Integer.parseInt(matcher.group(2));
			String fileId = matcher.group(3);
			DBS.getMcListener().handleDelete(sender,fileId);
			System.out.println("DELETE handled succesfully");
		}
		else System.out.print("Invalid DELETE received");
	}

}
