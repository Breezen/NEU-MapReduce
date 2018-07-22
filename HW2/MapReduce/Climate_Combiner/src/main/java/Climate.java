import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Climate {

    public static class TempMapper
            extends Mapper<Object, Text, Text, Temperature>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split(",");
            if (s[2].equals("TMIN")) {
                Text stationID = new Text(s[0]);
                Temperature temp = new Temperature(Long.parseLong(s[3]), 0, 1, 0);
                context.write(stationID, temp);
            }
            if (s[2].equals("TMAX")) {
                Text stationID = new Text(s[0]);
                Temperature temp = new Temperature(0, Long.parseLong(s[3]), 0, 1);
                context.write(stationID, temp);
            }
        }
    }

    public static class TempCombiner
            extends Reducer<Text,Temperature,Text,Temperature> {
        public void reduce(Text key, Iterable<Temperature> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sumTMIN = 0, sumTMAX = 0;
            long countTMIN = 0, countTMAX = 0;
            for (Temperature temp : values) {
                sumTMIN += temp.getSumTMIN();
                sumTMAX += temp.getSumTMAX();
                countTMIN += temp.getCountTMIN();
                countTMAX += temp.getCountTMAX();
            }
            Temperature temp = new Temperature(sumTMIN, sumTMAX, countTMIN, countTMAX);
            context.write(key, temp);
        }
    }

    public static class TempReducer
            extends Reducer<Text,Temperature,Text,Text> {
        public void reduce(Text key, Iterable<Temperature> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumTMIN = 0, sumTMAX = 0;
            long countTMIN = 0, countTMAX = 0;
            for (Temperature temp : values) {
                sumTMIN += temp.getSumTMIN();
                sumTMAX += temp.getSumTMAX();
                countTMIN += temp.getCountTMIN();
                countTMAX += temp.getCountTMAX();
            }
            String s = Double.toString(sumTMIN / countTMIN).concat(", ")
                             .concat(Double.toString(sumTMAX / countTMAX));
            Text result = new Text(s);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Climate");
        job.setJarByClass(Climate.class);
        job.setMapperClass(TempMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Temperature.class);
        job.setCombinerClass(TempCombiner.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
