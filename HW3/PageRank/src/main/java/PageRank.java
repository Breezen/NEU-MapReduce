import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageRank {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job preprocess = Job.getInstance(conf, "PreProcess");
        preprocess.setJarByClass(PageRank.class);
        preprocess.setMapperClass(Preprocessor.MyMapper.class);
        preprocess.setReducerClass(Preprocessor.MyReducer.class);
        preprocess.setOutputKeyClass(Text.class);
        preprocess.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(preprocess, new Path(args[0]));
        FileOutputFormat.setOutputPath(preprocess, new Path(args[1] + "/graph"));
        preprocess.waitForCompletion(true);

        for (Counter c : preprocess.getCounters().getGroup(Preprocessor.PAGERANK)) {
            conf.set(c.getName(), "" + c.getValue());
        }

        long nPage = Long.parseLong(conf.get("nPage"));
        double dangling = Double.parseDouble(conf.get("dangling")), delta = dangling / nPage;
        conf.set("delta", "" + delta * 1e10);
        conf.set("alpha", "0.15");
        conf.set("key.value.separator.in.input.line","\t");

        for (int i = 0; i < 10; ++i) {
            conf.set("iteration", "" + i);
            Job iterate = Job.getInstance(conf, "Iterate");
            iterate.setJarByClass(PageRank.class);
            iterate.setMapperClass(Iterator.MyMapper.class);
            iterate.setReducerClass(Iterator.MyReducer.class);
            iterate.setOutputKeyClass(Text.class);
            iterate.setOutputValueClass(Text.class);
            iterate.setInputFormatClass(KeyValueTextInputFormat.class);
            KeyValueTextInputFormat.addInputPath(iterate, new Path(args[1] + (i > 0 ? "/iteration/" + (i - 1) : "/graph")));
            FileOutputFormat.setOutputPath(iterate, new Path(args[1] + "/iteration/" + i));
            iterate.waitForCompletion(true);
            Counter c = iterate.getCounters().findCounter(Iterator.PAGERANK, "delta");
            conf.set("delta", "" + c.getValue());
        }

        Job rank = Job.getInstance(conf, "Rank");
        rank.setJarByClass(PageRank.class);
        rank.setMapperClass(Ranker.MyMapper.class);
        rank.setMapOutputKeyClass(DoubleWritable.class);
        rank.setMapOutputValueClass(Text.class);
        rank.setPartitionerClass(Ranker.MyPatitioner.class);
        rank.setSortComparatorClass(Ranker.MyComparator.class);
        rank.setReducerClass(Ranker.MyReducer.class);
        rank.setOutputKeyClass(Text.class);
        rank.setOutputValueClass(DoubleWritable.class);
        rank.setInputFormatClass(KeyValueTextInputFormat.class);
        KeyValueTextInputFormat.addInputPath(rank, new Path(args[1] + "/iteration/9"));
        FileOutputFormat.setOutputPath(rank, new Path(args[1] + "/ranking"));
        rank.waitForCompletion(true);
    }
}
