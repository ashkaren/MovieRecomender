import java.sql.*;
import java.util.*;

public class Database
{
    private Connection conn;
    private String user;
    private String pass;
    private String db;
    public String QUERY;

    public Database()
    {
        this("root", "", "brgoddard", false);
    }

    public Database(String username, String password, String database)
    {
        this(username, password, database, false);
    }

    public Database(String user, String pass, String db, boolean isStrict)
    {
        this.user = user;
        this.pass = pass;
        this.db = db;
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + this.db, this.user, this.pass);
            createSchemas();
        }
        catch(Exception e)
        {
            System.err.println(e);
            if(!isStrict)
            {
                createDatabase();
                try
                {
                    Class.forName("com.mysql.jdbc.Driver").newInstance();
                    this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db, user, pass);
                    createSchemas();
                }
                catch(Exception ex)
                {
                    System.err.println(ex);
                    System.exit(1);
                }
            }
        }
    }

    public void createDatabase()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/", this.user, this.pass);
            String sql = "CREATE DATABASE " + this.db;
            Statement cds = conn.createStatement();
            cds.executeUpdate(sql);
        }
        catch(Exception e)
        {
            System.err.println(e);
            System.exit(1);
        }
    }

    public void createSchemas()
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            this.conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/brgoddard", this.user, this.pass);
            Statement css = conn.createStatement();

            String Movie = "CREATE TABLE IF NOT EXISTS movie " +
                                        "(id INTEGER," +
                                        "title_english VARCHAR(255)," +
                                        "title_spanish VARCHAR(255)," +
                                        "image_url_imdb VARCHAR(255)," +
                                        "year INTEGER," +
                                        "rt_critic_rating DECIMAL," +
                                        "rt_critic_num_reviews INTEGER," +
                                        "rt_critic_num_fresh INTEGER," +
                                        "rt_critic_num_rotten INTEGER," +
                                        "rt_critic_score DECIMAL," +
                                        "rt_critic_top_rating DECIMAL," +
                                        "rt_critic_top_num_reviews INTEGER," +
                                        "rt_critic_top_num_fresh INTEGER," +
                                        "rt_critic_top_num_rotten INTEGER," +
                                        "rt_critic_top_score DECIMAL," +
                                        "rt_audience_rating DECIMAL," +
                                        "rt_audience_num_rating DECIMAL," +
                                        "rt_audience_score DECIMAL," +
                                        "image_url_rt VARCHAR(255)," +
                                        "PRIMARY KEY (id))";

            String Actor = "CREATE TABLE IF NOT EXISTS actor" +
                                    "(id VARCHAR(255)," +
                                    "actor_name VARCHAR(255)," +
                                    "PRIMARY KEY(id))";
            
            String Director = "CREATE TABLE IF NOT EXISTS director" +
                                        "(id VARCHAR(255)," +
                                        "director_name VARCHAR(255)," + 
                                        "PRIMARY KEY(id))";

            String Tag = "CREATE TABLE IF NOT EXISTS tag " +
                                        "(id INTEGER," +
                                        "tag_name VARCHAR(255)," +
                                        "PRIMARY KEY (id))";

            String MovieActor = "CREATE TABLE IF NOT EXISTS movie_actor " +
                                        "(movie_id INTEGER," +
                                        "actor_id VARCHAR(255)," +
                                        "actor_name VARCHAR(255)," +
                                        "ranking INTEGER," +
                                        "FOREIGN KEY (movie_id) REFERENCES movie(id)," + 
                                        "FOREIGN KEY (actor_id) REFERENCES actor(id))";

            String MovieDirector = "CREATE TABLE IF NOT EXISTS movie_director " +
                                            "(movie_id INTEGER," +
                                            "director_id VARCHAR(255)," +
                                            "director_name VARCHAR(255)," +
                                            "FOREIGN KEY (movie_id) REFERENCES movie(id)," + 
                                            "FOREIGN KEY (director_id) REFERENCES director(id))";

            String Genre = "CREATE TABLE IF NOT EXISTS movie_genre " +
                                        "(movie_id INTEGER," +
                                        "genre VARCHAR(255)," +
                                        "FOREIGN KEY (movie_id) REFERENCES movie(id))";

            String MovieTag = "CREATE TABLE IF NOT EXISTS movie_tag " +
                                        "(movie_id INTEGER," +
                                        "tag_id INTEGER," +
                                        "tag_weight INTEGER," +
                                        "FOREIGN KEY (tag_id) REFERENCES tag(id)," +
                                        "FOREIGN KEY (movie_id) REFERENCES movie(id))";
            
            String User = "CREATE TABLE IF NOT EXISTS user " +
                    					"(user_id INTEGER," +
                    					"movie_id INTEGER," +
                    					"rating DECIMAL," +
                    					"timestamp TIMESTAMP," +
                    					"FOREIGN KEY (movie_id) REFERENCES movie(id),";
            
            css.executeUpdate(Movie);
            css.executeUpdate(Actor);
            css.executeUpdate(Director);
            css.executeUpdate(Tag);
            css.executeUpdate(MovieTag);
            css.executeUpdate(MovieActor);
            css.executeUpdate(MovieDirector);
            css.executeUpdate(Genre);
            css.executeUpdate(User);
        }
        catch(Exception e)
        {
            System.err.println(e);
            System.exit(1);
        }
    }

    public boolean executeBatch(String input, String[][] values, int bufferSize)
    {
        int count = 0;
        PreparedStatement ps;
        try
        {
            ps = this.conn.prepareStatement(input);
            for(int i = 0; i < values.length; i++)
            {
                for(int j = 0; j < values[i].length; j++)
                {
                    if(values[i][j].equals("\\N"))
                    {
                        ps.setNull(j+1, Types.NULL);
                    }
                    else
                    {
                        ps.setString(j+1, values[i][j]);
                    }
                }
                ps.addBatch();

                if(++count % bufferSize == 0)
                {
                    ps.executeBatch();
                }
            }
            ps.executeBatch();
            ps.close();
            return true;
        }
        catch(Exception e)
        {
            System.err.println(e);
            System.exit(1);
        }
        return true;
    }
    //1
    public LinkedList<HashMap<String,String>> getTopMovies(int limit)
    {
        try
        {        
            String query = "SELECT title_english, title_spanish, rt_audience_score, year, image_url_rt, image_url_imdb, rt_critic_score FROM movie ORDER BY rt_audience_score DESC LIMIT 0, " + limit;
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("rt_audience_score", result.getString(3));
                map.put("year", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //2
    public LinkedList<HashMap<String,String>> getMovie(String title, int limit)
    {
        try
        {
            String query = "SELECT title_english, title_spanish, year, rt_audience_score, image_url_rt, image_url_imdb, rt_critic_score FROM movie WHERE title_english LIKE '%" + title + "%' LIMIT " + limit;
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String,String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("year", result.getString(3));
                map.put("rt_audience_score", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //3
    public LinkedList<HashMap<String,String>> getGenre(String genre, int limit)
    {
        try
        {
            String query = "SELECT title_english, title_spanish, year, rt_audience_score, image_url_rt, image_url_imdb, rt_critic_score FROM movie AS m, movie_genre AS g WHERE g.genre = '" + genre + "' AND m.id = g.movie_id ORDER BY rt_audience_score DESC LIMIT 0, " + limit;
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("year", result.getString(3));
                map.put("rt_audience_score", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //4
    public LinkedList<HashMap<String,String>> getDirector(String directorName)
    {
        try
        {
            String query = "SELECT title_english, title_spanish, year, rt_audience_score, image_url_rt, image_url_imdb, rt_critic_score FROM movie AS m, movie_director AS d WHERE d.director_name LIKE '%" + directorName + "%' AND m.id = d.movie_id";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("year", result.getString(3));
                map.put("rt_audience_score", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //5
    public LinkedList<HashMap<String,String>> getActor(String actorName)
    {
        try
        {
            String query = "SELECT title_english, title_spanish, year, rt_audience_score, image_url_rt, image_url_imdb, rt_critic_score FROM movie AS m, movie_actor AS a WHERE a.actor_name LIKE '%" + actorName + "%' AND m.id = a.movie_id";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("year", result.getString(3));
                map.put("rt_audience_score", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //6
    public LinkedList<HashMap<String, String>> getTag(String tagName)
    {
        try
        {
            String query = "SELECT m.title_english, m.title_spanish, m.year, m.rt_audience_score, m.image_url_rt, m.image_url_imdb, m.rt_critic_score FROM movie AS m, tag AS t, movie_tag AS mt WHERE t.id = mt.tag_id AND m.id = mt.movie_id AND t.tag_name = '" + tagName + "' ORDER BY avg(rt_audience_score) DESC";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("year", result.getString(3));
                map.put("rt_audience_score", result.getString(4));
                map.put("image_url_rt", result.getString(5));
                map.put("image_url_imdb", result.getString(6));
                map.put("rt_critic_score", result.getString(7));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //7
    public LinkedList<HashMap<String,String>> getTopDirectors(int limit, int input)
    {
    	int min = input-1;
        try
        {
            String query = "SELECT distinct md.director_name, avg(m.rt_audience_score) FROM movie_director AS md, movie AS m WHERE md.movie_id = m.id  group by md.movie_id HAVING count(*)>" + min + " order by avg(m.rt_audience_score) desc, md.director_name limit 10";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("director_name", result.getString(1));
                map.put("avg(m.rt_audience_score)", result.getString(2));
                list.add(map);
            }
            return list;
        }   
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //8
    public LinkedList<HashMap<String,String>> getTopActors(int limit, int input)
    {
    	int min = input-1;
        try
        {
            String query = "SELECT distinct ma.actor_name, avg(m.rt_audience_score) FROM movie_actor AS ma, movie AS m WHERE ma.movie_id = m.id group by ma.movie_id HAVING count(*)>" + min + " order by avg(m.rt_audience_score) desc, ma.actor_name limit 10";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String, String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("actor_name", result.getString(1));
                map.put("avg(m.rt_audience_score)", result.getString(2));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //9
    public LinkedList<HashMap<String,String>> getTimeline(int user)
    {
        try
        {
            String query = "SELECT m.title_english, m.title_spanish, u.rating, mg.genre, (Count(mg.genre)*100/(SELECT Count(*) FROM movie_genre AS mg) AS Percentage FROM movie AS m, user AS u, movie_genre AS mg WHERE u.user_id = " + user + " AND u.movie_id = m.id AND m.id = mg.movie_id order by u.timestamp DESC";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String,String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("title_english", result.getString(1));
                map.put("title_spanish", result.getString(2));
                map.put("rating", result.getString(3));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
    //10
    public LinkedList<HashMap<String,String>> getMovieTags(int movie)
    {
        try
        {
            String query = "SELECT distinct t.tag_name FROM tag AS t, movie_tag AS mt WHERE mt.movie_id = " + movie + " AND mt.tag_id = t.id";
            Statement s = this.conn.createStatement();
            ResultSet result = s.executeQuery(query);
            LinkedList<HashMap<String,String>> list = new LinkedList<HashMap<String,String>>();
            HashMap<String, String> map;
            while(result.next())
            {
                map = new HashMap<String, String>();
                map.put("tag_name", result.getString(1));
                list.add(map);
            }
            return list;
        }
        catch(Exception e)
        {
            System.err.println(e);
            return null;
        }
    }
}
