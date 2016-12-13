import java.io.*;
import java.util.*;

public abstract class Reader
{
    BufferedReader reader;
    ArrayList<String> lines;

    public Reader(String input)
    {
        this.lines = new ArrayList<String>();
        try
        {
            this.reader = new BufferedReader(new FileReader(input));
        }
        catch(Exception e)
        {
            this.reader = null;
            System.err.println(e);
            System.exit(1);
        }

        try
        {
            boolean isFirst = true;
            String line;
            while((line = this.reader.readLine()) != null)
            {
                if(isFirst)
                {
                    isFirst = false;
                    continue;
                }
                this.lines.add(line);
            }
        }
        catch(Exception e)
        {
            System.err.println(e);
            System.exit(1);
        }
    }
    
    public abstract String[][] insertItems();
}
