public class ReaderActor extends Reader
{
    public static final String QUERY = "insert into movie_actor (movie_id, actor_id, actor_name, ranking) values (?, ?, ?, ?)";

    public ReaderActor(String input)
    {
        super(input);
    }

    public String[][] insertItems()
    {
        String[][] items = new String[this.lines.size()][4];
        for(int i = 0; i < this.lines.size(); i++)
        {
            String[] tokens = this.lines.get(i).split("\t");
            items[i][0] = tokens[0];
            items[i][1] = tokens[1];
            items[i][2] = tokens[2];
            items[i][3] = tokens[3];
        }
        return items;
    }
}
