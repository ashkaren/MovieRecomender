public class ReaderDirector extends Reader
{
    public static final String QUERY = "insert into movie_director (movie_id, director_id, director_name) values (?, ?, ?)";

    public ReaderDirector(String file)
    {
        super(file);
    }

    public String[][] insertItems()
    {
        String[][] items = new String[this.lines.size()][3];
        for(int i = 0; i < this.lines.size(); i++)
        {
            String[] tokens = this.lines.get(i).split("\t");
            items[i][0] = tokens[0];
            items[i][1] = tokens[1];
            items[i][2] = tokens[2];
        }
        return items;
    }
}
