public class ReaderGenre extends Reader
{
    public static final String QUERY = "insert into movie_genre (movie_id, genre) values (?, ?)";

    public ReaderGenre(String input)
    {
        super(input);
    }

    public String[][] insertItems()
    {
        String[][] items = new String[this.lines.size()][2];
        for(int i = 0; i < this.lines.size(); i++)
        {
            String[] tokens = this.lines.get(i).split("\t");
            items[i][0] = tokens[0];
            items[i][1] = tokens[1];
        }
        return items;
    }
}
