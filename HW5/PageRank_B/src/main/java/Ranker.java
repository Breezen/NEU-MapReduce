import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Ranker {
    public static class MyMapper extends Mapper<Object, Text, DoubleWritable, IntWritable> {
        protected void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split(":");
            int pageID = Integer.parseInt(s[0]);
            double score = Double.parseDouble(s[1]);
            context.write(new DoubleWritable(score), new IntWritable(pageID));
        }
    }

    public static class MyPatitioner extends Partitioner<DoubleWritable, IntWritable> {
        public int getPartition(DoubleWritable key, IntWritable value, int numberOfReducer) {
            return 0;
        }
    }

    public static class MyComparator extends WritableComparator {
        public MyComparator() {
            super(DoubleWritable.class, true);
        }

        public int compare(WritableComparable w1, WritableComparable w2) {
            return -w1.compareTo(w2);
        }
    }

    public static class MyReducer extends Reducer<DoubleWritable, IntWritable, Text, DoubleWritable> {
        private int count = 0;
        private String[] indexToPage;

        protected void setup(Context context) throws IOException, InterruptedException {
            int nPage = Integer.parseInt(context.getConfiguration().get("nPage"));
            indexToPage = new String[nPage];
            for (String line : FileUtils.readLines(new File("./mapping"))) {
                String[] s = line.split(":");
                int pageID = Integer.parseInt(s[0]);
                indexToPage[pageID] = s[1];
            }
        }

        protected void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context
        ) throws IOException, InterruptedException {
            for (IntWritable v : values) {
                if (count < 100) {
                    context.write(new Text(indexToPage[v.get()]), key);
                    ++count;
                } else {
                    return;
                }
            }
        }
    }
}
