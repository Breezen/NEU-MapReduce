import java.io.IOException;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Iterator {

    public static class MyMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {
        private int nIter, nPage;
        private double alpha, vec[];

        protected void setup(Context context) throws IOException, InterruptedException {
            nIter = Integer.parseInt(context.getConfiguration().get("iteration"));
            nPage = Integer.parseInt(context.getConfiguration().get("nPage"));
            alpha = Double.parseDouble(context.getConfiguration().get("alpha"));
            vec = new double[nPage];
            for (String line : FileUtils.readLines(new File("./pr" + (nIter - 1)))) {
                String[] s = line.split(":");
                int id = Integer.parseInt(s[0]);
                double score = Double.parseDouble(s[1]);
                vec[id] = score;
            }
        }

        protected void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String row = value.toString();
            int splitidx = row.indexOf(':');
            int pageID = Integer.parseInt(row.substring(0, splitidx));
            double res = 0;
            for (String s : row.substring(splitidx + 1).split("\\|")) {
                String[] col = s.split(",");
                res += Double.parseDouble(col[1]) * vec[Integer.parseInt(col[0])];
            }
            res = res * (1 - alpha) + alpha / nPage;
            context.write(new IntWritable(pageID), new DoubleWritable(res));
        }
    }

    public static class MyPartitioner extends Partitioner<IntWritable, DoubleWritable> {
        public int getPartition(IntWritable key, DoubleWritable value, int i) {
            return 0;
        }
    }

    public static class MyReducer extends Reducer<IntWritable, DoubleWritable, NullWritable, Text> {
        protected void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context
        ) throws IOException, InterruptedException {
            for (DoubleWritable score : values) {
                context.write(NullWritable.get(), new Text(key.toString() + ':' + score.toString()));
                break;
            }
        }
    }
}
