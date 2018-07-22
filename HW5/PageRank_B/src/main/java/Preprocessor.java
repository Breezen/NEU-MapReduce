import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public static class MyPartitioner extends Partitioner<Text, Text> {
        public int getPartition(Text key, Text value, int i) {
            return 0;
        }
    }

    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        private int nPage = 0;
        private MultipleOutputs out;
        private Map<String, Integer> pageIndex = new HashMap<>();
        private Map<Integer, Integer> nOutlinks = new HashMap<>();
        private Map<Integer, List<Integer>> outlinks = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            out = new MultipleOutputs<>(context);
        }

        protected void reduce(Text key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException{
            String page = key.toString();
            if (!pageIndex.containsKey(page)) {
                pageIndex.put(page, nPage++);
            }
            int pageID = pageIndex.get(page);
            for (Text v : values) {
                String s = v.toString();
                String[] links = s.substring(1, s.length() - 1).split(", ");
                if (links.length > 0) {
                    nOutlinks.put(pageIndex.get(page), links.length);
                    List<Integer> t = new ArrayList<>();
                    for (String link : links) {
                        if (link.equals("")) continue;
                        if (!pageIndex.containsKey(link)) {
                            pageIndex.put(link, nPage++);
                        }
                        int linkID = pageIndex.get(link);
                        t.add(linkID);
                    }
                    outlinks.put(pageID, t);
                }
                break;
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // pageID:pageName
            for (String page : pageIndex.keySet()) {
                out.write("output", NullWritable.get(), new Text(pageIndex.get(page).toString() + ':' + page), "mapping/mapping");
            }
            // Dangling nodes
            for (int i = 0; i < nPage; ++i) {
                if (!nOutlinks.containsKey(i)) {
                    outlinks.put(i, new ArrayList<>());
                    for (int j = 0; j < nPage; ++j) {
                        outlinks.get(i).add(j);
                    }
                    nOutlinks.put(i, nPage);
                }
            }
            // pageID:outlinkID,v|outlinkID,v|...
            for (int pageID : outlinks.keySet()) {
                StringBuilder s = new StringBuilder(Integer.toString(pageID) + ':');
                List<Integer> links = outlinks.get(pageID);
                double v = 1.0 / nOutlinks.get(pageID);
                for (int linkID : links) {
                    s.append(linkID).append(',').append(v).append('|');
                }
                out.write("output", NullWritable.get(), new Text(s.substring(0, s.length() - 1)), "matrix/matrix");
            }
            // init pagerank
            // pageID:pagerank
            double v = 1.0 / nPage;
            for (int i = 0; i < nPage; ++i) {
                out.write("output", NullWritable.get(), new Text("" + i + ":" + v), "iteration/0/part");
            }
            out.close();
            context.getCounter(PAGERANK, "nPage").increment(nPage);
        }
    }
}