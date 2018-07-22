import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Iterator {
    public static final String PAGERANK = "pagerank";

    public static class MyMapper extends Mapper<Text, Text, Text, Text> {
        private Boolean firstIter;
        private long nPage;
        private double alpha, delta;

        protected void setup(Context context) {
            firstIter = Integer.parseInt(context.getConfiguration().get("iteration")) == 0;
            nPage = Long.parseLong(context.getConfiguration().get("nPage"));
            alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
            delta = firstIter ? 0 : Double.parseDouble(context.getConfiguration().get("delta")) / 1e10;
        }

        protected void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split("\\|");
            double score = firstIter ? (1.0 / nPage) : Double.parseDouble(s[0]);
            score += (1.0 - alpha) * delta / nPage;
            context.write(key, new Text("" + score + "|" + s[1]));
            String[] links = s[1].substring(1, s[1].length() - 1).split(", ");
            if (links.length > 0) {
                double p = score / links.length;
                for (String l : links) {
                    if (!l.isEmpty()) {
                        context.write(new Text(l), new Text("" + p));
                    }
                }
            } else {
                context.getCounter(PAGERANK, "delta").increment((long)(score * 1e10));
            }
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private long nPage;
        private double alpha;

        protected void setup(Context context) {
            nPage = Long.parseLong(context.getConfiguration().get("nPage"));
            alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
        }

        protected void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            String links = "[]";
            double score = 0.0;
            for (Text v : values) {
                String s = v.toString();
                if (s.contains("|")) {
                    links = s.split("\\|")[1];
                } else {
                    score += Double.parseDouble(s);
                }
            }
            score = alpha / nPage + (1.0 - alpha) * score;
            String v = "" + score + "|" + links;
            context.write(key, new Text(v));
        }
    }
}
