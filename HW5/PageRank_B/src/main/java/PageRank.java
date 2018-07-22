import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.net.URI;

public class PageRank {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Build Mapping: page->id
        // Build Sparse Matrix
        // Build Init PageRank vector
        Job preprocess = Job.getInstance(conf, "PreProcess");
        preprocess.setJarByClass(PageRank.class);
        preprocess.setMapperClass(Preprocessor.MyMapper.class);
        preprocess.setMapOutputKeyClass(Text.class);
        preprocess.setMapOutputValueClass(Text.class);
        preprocess.setPartitionerClass(Preprocessor.MyPartitioner.class);
        preprocess.setNumReduceTasks(1);
        preprocess.setReducerClass(Preprocessor.MyReducer.class);
        preprocess.setOutputKeyClass(NullWritable.class);
        preprocess.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(preprocess, new Path(args[0]));
        FileOutputFormat.setOutputPath(preprocess, new Path(args[1]));
        MultipleOutputs.addNamedOutput(preprocess, "output", TextOutputFormat.class, NullWritable.class, Text.class);
        preprocess.waitForCompletion(true);

        // Broadcast parameters
        Counter c = preprocess.getCounters().findCounter(Preprocessor.PAGERANK, "nPage");
        long nPage = c.getValue();
        conf.set("nPage", "" + nPage);
        conf.set("alpha", "0.15");

        // PageRank Iterations
        for (int i = 1; i <= 10; ++i) {
            conf.set("iteration", "" + i);
            Job iterate = Job.getInstance(conf, "Iterate");
            iterate.setJarByClass(PageRank.class);
            iterate.addCacheFile(new URI("s3://seanxwang-pagerank/output/iteration/" + (i - 1) + "/part-r-00000#pr" + (i - 1)));
            iterate.setMapperClass(Iterator.MyMapper.class);
            iterate.setMapOutputKeyClass(IntWritable.class);
            iterate.setMapOutputValueClass(DoubleWritable.class);
            iterate.setPartitionerClass(Iterator.MyPartitioner.class);
            iterate.setNumReduceTasks(1);
            iterate.setReducerClass(Iterator.MyReducer.class);
            iterate.setOutputKeyClass(NullWritable.class);
            iterate.setOutputValueClass(Text.class);
            TextInputFormat.addInputPath(iterate, new Path(args[1] + "/matrix"));
            FileOutputFormat.setOutputPath(iterate, new Path(args[1] + "/iteration/" + i));
            iterate.waitForCompletion(true);
        }

        // Ranking
        Job rank = Job.getInstance(conf, "Rank");
        rank.setJarByClass(PageRank.class);
        rank.addCacheFile(new URI("s3://seanxwang-pagerank/output/mapping/mapping-r-00000#mapping"));
        rank.setMapperClass(Ranker.MyMapper.class);
        rank.setMapOutputKeyClass(DoubleWritable.class);
        rank.setMapOutputValueClass(IntWritable.class);
        rank.setPartitionerClass(Ranker.MyPatitioner.class);
        rank.setSortComparatorClass(Ranker.MyComparator.class);
        rank.setReducerClass(Ranker.MyReducer.class);
        rank.setOutputKeyClass(Text.class);
        rank.setOutputValueClass(DoubleWritable.class);
        TextInputFormat.addInputPath(rank, new Path(args[1] + "/iteration/10"));
        FileOutputFormat.setOutputPath(rank, new Path(args[1] + "/ranking"));
        rank.waitForCompletion(true);
    }
}
