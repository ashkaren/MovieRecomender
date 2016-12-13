import java.util.*;
import java.io.*;
import java.util.regex.Matcher;

public class Main
{
    public static void main(String[] args)
    {
        Scanner scan = new Scanner(System.in);
        String[][] movies;
        String[][] actors;
        String[][] directors;
        String[][] genres;
        String[][] tags;
        String[][] tagItems;
        String[][] users;
        String[] queries = new String[7];
        boolean isWritingFile = false;

        System.out.println("Create Sql files? (y/n)");
        String ans = scan.next();
        if(ans.equals("y"))
        {
            isWritingFile = true;
        }

        //Reads in the dataset from rotten tomatoes.
        Reader read = new ReaderMovie("../DB Project 2016/dataset/movies.dat");
        movies = read.insertItems();
        queries[0] = ((ReaderMovie) read).QUERY;
        read = new ReaderActor("../DB Project 2016/dataset/movie_actors.dat");
        actors = read.insertItems();
        queries[1] = ((ReaderActor) read).QUERY;
        read = new ReaderDirector("../DB Project 2016/dataset/movie_directors.dat");
        directors = read.insertItems();
        queries[2] = ((ReaderDirector) read).QUERY;
        read = new ReaderGenre("../DB Project 2016/dataset/movie_genres.dat");
        genres = read.insertItems();
        queries[3] = ((ReaderGenre) read).QUERY;
        read = new ReaderTag("../DB Project 2016/dataset/movie_tags.dat");
        tags = read.insertItems();
        queries[4] = ((ReaderTag) read).QUERY;
        read = new ReaderTagItem("../DB Project 2016/dataset/tags.dat");
        tagItems = read.insertItems();
        queries[5] = ((ReaderTagItem) read).QUERY;
        read = new ReaderTagItem("../DB Project 2016/dataset/user_ratedmovies-timestamps.dat");
        users = read.insertItems();
        queries[6] = ((ReaderTagItem) read).QUERY;
        
        //Filters duplicates from actors
        HashMap<String, String> map = new HashMap<String, String>();
        for(int i = 0; i < actors.length; i++)
        {
            if(map.containsKey(actors[i][1]))
            {
                continue;
            }
            map.put(actors[i][1], actors[i][2]);
        }
        String[][] actorsS = new String[map.size()][2];
        int index = 0;
        for (String key : map.keySet())
        {
        	actorsS[index][0] = key;
        	actorsS[index][1] = map.get(key);
            index++;
        }
        
        //Filters duplicates from directors
        map = new HashMap<String,String>();
        for(int i = 0; i < directors.length; i++)
        {
            if(map.containsKey(directors[i][1]))
            {
                continue;
            }
            map.put(directors[i][1], directors[i][2]);
        }
        String[][] directorsS = new String[map.size()][2];
        index = 0;
        for(String key : map.keySet())
        {
        	directorsS[index][0] = key;
        	directorsS[index][1] = map.get(key);
            index++;
        }

        //Creates SQL files if true.
        if(isWritingFile)
        {
            createSqlFile(movies, "movie", queries[0]);
            createSqlFile(actors, "movie_actor", queries[1]);
            createSqlFile(directors, "movie_director", queries[2]);
            createSqlFile(genres, "movie_genre", queries[3]);
            createSqlFile(tags, "movie_tag", queries[4]);
            createSqlFile(tagItems, "tag", queries[5]);
            createSqlFile(users, "user", queries[6]);
            createSqlFile(actorsS, "actor", "insert into actor (id, actor_name) values (?,?)");
            createSqlFile(directorsS, "director", "insert into director (id, director_name) values (?,?)");
        }
        
        //Gets server's name, user and password.
        String user = null;
        String pass = null;
        String dbName = null;
        System.out.println("Enter MySQL username: ");
        user = scan.next();
        System.out.println("Enter MySQL password: ");
        pass = scan.next();
        System.out.println("Enter MySQL DB name: ");
        dbName = scan.next();
        
        //Creates database.
        Database db;
        if(user != null && pass != null && dbName != null)
        {
            db = new Database(user, pass, dbName, true);
        }
        else
        {
            db = new Database();
        }

        System.out.println("Insert data files into database? This will take a few hours. (y/n)");
        ans = scan.next();
        if(ans.equals("n"))
        {
            System.exit(0);
        }
        scan.close();

        System.out.println("Movies...");
        //db.executeBatch(queries[0], movies, 2000);
        System.out.println("Actors...");
        //db.executeBatch("insert into actor (id, actor_name) values (?,?)", actorsS, 2000);
        System.out.println("Directors...");
        //db.executeBatch("insert into director (id, director_name) values (?,?)", directorsS, 2000);
        System.out.println("Tags...");
        //db.executeBatch(queries[5], tagItems, 1000);
        System.out.println("Movie actors...");
        //db.executeBatch(queries[1], actors, 2000);
        System.out.println("Movie directors...");
        //db.executeBatch(queries[2], directors, 2000);
        System.out.println("Genres...");
        //db.executeBatch(queries[3], genres, 2000);
        System.out.println("Movie tags...");
        //db.executeBatch(queries[4], tags, 2000);
        System.out.println("Users...");
        db.executeBatch(queries[6], users, 2000);
        System.out.println("Database complete.");
    }

    private static boolean createSqlFile(String[][] table, String filename, String QUERY)
    {
        File dir = new File("../DB Project 2016/sql");
        if(!dir.exists())
        {
            if(!dir.mkdir())
            {
                System.out.println("Error creating directory.");
                return false;
            }
        }

        File file = new File("../DB Project 2016/sql/" + filename + ".sql");
        try
        {
            file.createNewFile();
        }
        catch(Exception e)
        {
            System.out.println("Could not create SQL file.");
            return false;
        }

        try
        {
            FileWriter fw = new FileWriter(file);
            BufferedWriter writer = new BufferedWriter(fw);
            for(int i = 0; i < table.length; i++)
            {
                String tempQUERY = QUERY;
                for(int j = 0; j < table[i].length; j++)
                {
                    tempQUERY = tempQUERY.replaceFirst("\\?", "'" + Matcher.quoteReplacement(table[i][j].replaceAll("'","\\\\'")) + "'").replaceAll("'\\\\N'", "null");
                }
                try
                {
                    writer.write(tempQUERY);
                    writer.newLine();
                }
                catch(Exception e)
                {
                    System.out.println("Could not write:\t" + tempQUERY);
                }
            }
            writer.close();
        }
        catch(Exception e)
        {
            System.out.println(e);
            return false;
        }
        return true;
    }
}
