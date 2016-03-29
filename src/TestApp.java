import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;
import java.util.Map;

class TestApp {
	
	private static String host;
	private static String remote_object_name;

	private static final Map<String, Integer> sub_protocols;
    static
    {
    	sub_protocols = new HashMap<String, Integer>();
    	sub_protocols.put("backup", 2);
    	sub_protocols.put("restore", 1);
    	sub_protocols.put("delete", 1);
    	sub_protocols.put("reclaim", 1);
    }
    
    private static void printUsage()
    {
    	System.out.println("Usage: java TestApp <peer_ap> <sub_protocol> <opnd_1> [opnd_2]");
    }
	
	public static void main(String[] args) {
		if (args.length < 3 || args.length > 4
				|| !sub_protocols.get(args[1].toLowerCase()).equals(args.length-2))
		{
			printUsage();
			return;
		}
		
		if (args[0].contains(":")) {
			String[] split = args[0].split(":");
			host = split[0];
			remote_object_name = split[1];
		}
		else {
			host = "127.0.0.1";
			remote_object_name = args[0];
		}
		
		try {
			Registry registry = LocateRegistry.getRegistry(host);
			ServerInterface stub = (ServerInterface) registry.lookup(remote_object_name);
			switch (args[1].toLowerCase())
			{
			case "backup":
				stub.backup(args[2], Integer.parseInt(args[3]));
				System.out.println("Backup successful");
				break;
			case "restore":
				stub.restore(args[2]);
				System.out.println("Restore successful");
				break;
			case "delete":
				stub.delete(args[2]);
				System.out.println("Delete successful");
				break;
			case "reclaim":
				stub.spaceReclaiming(Long.parseLong(args[2]));
				System.out.println("Space reclaiming successful");
				break;
			default:
				printUsage();
				return;
			}
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
		}
	}
}
