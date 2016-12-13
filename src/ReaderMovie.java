public class ReaderMovie extends Reader
{
    public static final String QUERY = "INSERT INTO movie (id, title_english, title_spanish, image_url_imdb, year, rt_critic_rating, rt_critic_num_reviews, rt_critic_num_fresh, rt_critic_num_rotten, rt_critic_score, rt_critic_top_rating, rt_critic_top_num_reviews, rt_critic_top_num_fresh, rt_critic_top_num_rotten, rt_critic_top_score, rt_audience_rating, rt_audience_num_rating, rt_audience_score, image_url_rt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    public ReaderMovie(String input)
    {
        super(input);
    }

    public String[][] insertItems()
    {
        String[][] items = new String[this.lines.size()][19];
        for(int i = 0; i < this.lines.size(); i++)
        {
            String[] tokens = this.lines.get(i).split("\t");
            items[i][0] = tokens[0];
            items[i][1] = tokens[1];
            items[i][2] = tokens[3];
            items[i][3] = tokens[4];
            items[i][4] = tokens[5];
            items[i][5] = tokens[7];
            items[i][6] = tokens[8];
            items[i][7] = tokens[9];
            items[i][8] = tokens[10];
            items[i][9] = tokens[11];
            items[i][10] = tokens[12];
            items[i][11] = tokens[13];
            items[i][12] = tokens[14];
            items[i][13] = tokens[15];
            items[i][14] = tokens[16];
            items[i][15] = tokens[17];
            items[i][16] = tokens[18];
            items[i][17] = tokens[19];
            items[i][18] = tokens[20];
        }
        return items;
    }
}
