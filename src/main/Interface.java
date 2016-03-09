package main;
import java.io.IOException;
import java.util.Scanner;

import tasks.Backup;
import tasks.Delete;
import tasks.Restore;
import tasks.SpaceReclaiming;

public class Interface {
	public static void main(String[] args) {
		try {
			if (args.length == 0)
				new DBS("224.0.0.0",4445,"224.0.0.0",4446,"224.0.0.0",4447).run();
			else if (args.length == 1)
				new DBS(Integer.parseInt(args[0]),"224.0.0.0",4445,"224.0.0.0",4446,"224.0.0.0",4447).run();
				
			Scanner scanner = new Scanner(System.in);
			boolean end = false;
			while (!end)
			{
				System.out.println("Choose: 0:Exit, 1:Backup, 2:Restore, 3:Delete, 4: Reclaim space");
				if (scanner.hasNextInt())
				{
					int choice = scanner.nextInt();
					if (choice == 0) 
						end = true;
					else if (choice == 1)
						new Backup("imagem.png",1).run();
					else if (choice == 2)
						new Restore("imagem.png","0428750a077b18b6ed3aacd79bcf48687396b7deb2ddd525ee1c398edcbfa094").run();
					else if (choice == 3)
						new Delete("0428750a077b18b6ed3aacd79bcf48687396b7deb2ddd525ee1c398edcbfa094").run();
					else if (choice == 4 && scanner.hasNextInt())
						new SpaceReclaiming(scanner.nextInt()).run();
				}
				else scanner.next();
			}
			scanner.close();
			
		} catch (IOException e) {
			System.err.println("Error while starting peer");
			e.printStackTrace();
		}
		
	}
}
