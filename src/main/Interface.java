package main;
import java.io.IOException;
import java.util.Scanner;

import tasks.Backup;

public class Interface {
	public static void main(String[] args) {
		try {
			new DBS("224.0.0.3",4445,"224.0.0.3",4446,"224.0.0.3",4447).run();

			Scanner scanner = new Scanner(System.in);
			while (true)
			{
				System.out.println("Choose: 0:Exit, 1:Backup, 2:Restore, ... ");
				if (scanner.hasNextInt())
				{
					int choice = scanner.nextInt();
					if (choice == 0) 
						break;
					else if (choice == 1)
						new Thread(new Backup("1.zip",3)).start();
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
