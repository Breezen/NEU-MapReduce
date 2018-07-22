import java.io.IOException;
import java.util.HashMap;
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

        private HashMap<String, Temperature> station = new HashMap<>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split(",");
            if (s[2].equals("TMIN") || s[2].equals("TMAX")) {
                if (station.containsKey(s[0])) {
                    Temperature oldT = station.get(s[0]),
                                newT = new Temperature();
                    if (s[2].equals("TMIN")) {
                        newT.set(oldT.getSumTMIN() + Long.parseLong(s[3]),
                                 oldT.getSumTMAX(),
                                 oldT.getCountTMIN() + 1,
                                 oldT.getCountTMAX());
                    } else {
                        newT.set(oldT.getSumTMIN(),
                                 oldT.getSumTMAX() + Long.parseLong(s[3]),
                                 oldT.getCountTMIN(),
                                 oldT.getCountTMAX() + 1);
                    }
                    station.replace(s[0], newT);
                } else {
                    Temperature temp = new Temperature();
                    if (s[2].equals("TMIN")) {
                        temp.set(Long.parseLong(s[3]), 0, 1, 0);
                    } else {
                        temp.set(0, Long.parseLong(s[3]), 0, 1);
                    }
                    station.put(s[0], temp);
                }
            }
        }

        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            for (String s : station.keySet()) {
                Text stationID = new Text(s);
                context.write(stationID, station.get(s));
            }
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
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
