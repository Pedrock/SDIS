package main;
import java.io.IOException;
import java.util.Scanner;

import tasks.Backup;
import tasks.Delete;
import tasks.Restore;

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
				System.out.println("Choose: 0:Exit, 1:Backup, 2:Restore, 3:Delete, ... ");
				if (scanner.hasNextInt())
				{
					int choice = scanner.nextInt();
					if (choice == 0) 
						end = true;
					else if (choice == 1)
						new Backup("imagem.png",1).run();
					else if (choice == 2)
						new Restore("imagem.png","16b82104e89286df8f8303ee810a74155412b4c678b1118d71e2e9406bdf23c2").run();
					else if (choice == 3)
						new Delete("16b82104e89286df8f8303ee810a74155412b4c678b1118d71e2e9406bdf23c2").run();
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
