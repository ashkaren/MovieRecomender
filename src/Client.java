//This is basically what needs the GUI
import java.util.*;

public class Client 
{
	public static void main(String[] args)
    {
        Scanner scan = new Scanner(System.in);
        String username = null;
        String password = null;
        String databaseName = null;

        //Asks for server connection info.
        System.out.println("Enter DB name: ");
        databaseName = scan.next();
        System.out.println("Is there a password? (y/n)");
        String answer = scan.next();
        if(answer.equals("y"))
        {
            System.out.println("Enter username: ");
            username = scan.next();
            System.out.println("Enter password: ");
            password = scan.next();
        }
        Database db;
        if(username == null || password == null)
        {
            db = new Database();
        }
        else
        {
            db = new Database(username, password, databaseName, true);
        }

        //Query switch statement. Input/output self explanatory by printlns.
        System.out.println("\nWelcome!");
        while(true)
        {
            System.out.println("\n[1] Top Popular Movies");
            System.out.println("[2] Search by Title");
            System.out.println("[3] Search by Genre");
            System.out.println("[4] Search by Director");
            System.out.println("[5] Search by Actor");
            System.out.println("[6] Search by Tag");
            System.out.println("[7] Top 10 Directors");
            System.out.println("[8] Top 10 Actors");
            System.out.println("[9] User Timeline");
            System.out.println("[10] Movie Tags");
            System.out.println("[0] Exit");

            int command = scan.nextInt();
            scan.nextLine();

            LinkedList<HashMap<String,String>> result;
            switch(command)
            {
                case 1:     
                			System.out.println("Enter a number of movies:");
                			int k1 = scan.nextInt();
                            result = db.getTopMovies(k1);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));
                                }
                            }
                            break;
                case 2:     
                            System.out.println("Enter a movie title:");
                            String title = scan.nextLine();
                            result = db.getMovie(title, 1);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));   
                                }
                            }
                            break;
                case 3:     
                            System.out.println("Enter a genre:");
                            String genre = scan.nextLine();
                            System.out.println("Enter a number of movies:");
                            int k2 = scan.nextInt();
                            result = db.getGenre(genre, k2);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));   
                                }
                            }
                            break;
                case 4:     
                            System.out.println("Enter a director name:");
                            String directorName = scan.nextLine();
                            result = db.getDirector(directorName);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));   
                                }
                            }
                            break;
                case 5:     
                            System.out.println("Enter an actor name:");
                            String actorName = scan.nextLine();
                            result = db.getActor(actorName);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));   
                                }
                            }
                            break;
                case 6:     
                            System.out.println("Enter a tag:");
                            String tagName = scan.nextLine();
                            result = db.getTag(tagName);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));   
                                }
                            }
                            break;
                case 7:     
                			System.out.println("Enter a minimum number of movies:");
                			int k3 = scan.nextInt();
                            result = db.getTopDirectors(10, k3);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));
                                }
                            }
                            break;
                case 8:     
                			System.out.println("Enter a minimum number of movies:");
                			int k4 = scan.nextInt();
                            result = db.getTopActors(10, k4);
                            for(HashMap<String,String> map : result)
                            {
                                for(String key : map.keySet())
                                {
                                    System.out.printf("%s\t\t", map.get(key));
                                }
                            }
                            break;
                case 9:     
                			System.out.println("Enter a user id:");
                			int userID = scan.nextInt();
                			result = db.getTimeline(userID);
                			for(HashMap<String,String> map : result)
                			{
                				for(String key : map.keySet())
                				{
                					System.out.printf("%s\t\t", map.get(key));
                				}
                			}
            		break;
                case 10:     
                    		System.out.println("Enter a movie id:");
                    		int movieID = scan.nextInt();
                            result = db.getMovieTags(movieID);
                    		for(HashMap<String,String> map : result)
                    		{
                    			for(String key : map.keySet())
                    			{
                    				System.out.printf("%s\t\t", map.get(key));
                    			}
                    		}
                    		break;
                case 0:     
                            System.exit(0);
                default:    System.out.println("Invalid number.");
                            continue;
            }
            System.out.printf("\n");
        }
    }
}
