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
            if (s[2].equals("TMIN") || s[2].equals("TMAX")) {
                Text stationID = new Text(s[0]);
                Temperature temp = new Temperature(s[2].equals("TMIN"), Long.parseLong(s[3]));
                context.write(stationID, temp);
            }
        }
    }

    public static class TempReducer
            extends Reducer<Text,Temperature,Text,Text> {
        public void reduce(Text key, Iterable<Temperature> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sumMin = 0, sumMax = 0;
            long countMin = 0, countMax = 0;
            for (Temperature temp : values) {
                if (temp.isLow()) {
                    sumMin += temp.getT();
                    countMin++;
                } else {
                    sumMax += temp.getT();
                    countMax++;
                }
            }
            String s = Double.toString(sumMin / countMin).concat(", ")
                             .concat(Double.toString(sumMax / countMax));
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
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
