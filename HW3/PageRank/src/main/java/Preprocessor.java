import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Preprocessor {
    public static final String PAGERANK = "pagerank";
    static Bz2WikiParser parser = new Bz2WikiParser();

    public static class MyMapper extends Mapper<Object, Text, Text, Text> {
        protected void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException{
            String line = value.toString();
            String parsed = parser.parse(line);
            if (!parsed.equals("SKIP")) {
                String[] result = parsed.split("~");
                context.write(new Text(result[0]), new Text(result[1]));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException{
            context.getCounter(PAGERANK, "nPage").increment(1);
            for (Text v: values) {
                String s = v.toString();
                if (s.equals("[]")) {
                    context.getCounter(PAGERANK, "dangling").increment(1);
                }
                context.write(key, new Text("0|" + s));
            }
        }
    }
}