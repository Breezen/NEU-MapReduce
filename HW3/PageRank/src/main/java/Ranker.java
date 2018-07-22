import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

public class Ranker {
    public static class MyMapper extends Mapper<Text, Text, DoubleWritable, Text> {
        protected void map(Text key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split("\\|");
            Double score = Double.parseDouble(s[0]);
            context.write(new DoubleWritable(score), key);
        }
    }

    public static class MyPatitioner extends Partitioner<DoubleWritable, Text> {
        public int getPartition(DoubleWritable key, Text value, int numberOfReducer) {
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

    public static class MyReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {
        private int count = 0;

        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context
        ) throws IOException, InterruptedException {
            for (Text v : values) {
                if (count < 100) {
                    context.write(v, key);
                    ++count;
                } else {
                    return;
                }
            }
        }
    }
}
