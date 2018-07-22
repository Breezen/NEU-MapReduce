import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Climate {

    public static class TempMapper
            extends Mapper<Object, Text, StationYear, Temperature>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] s = value.toString().split(",");
            if (s[2].equals("TMIN") || s[2].equals("TMAX")) {
                int year = Integer.parseInt(s[1].substring(0, 4));
                StationYear sy = new StationYear(s[0], year);
                Temperature temp = s[2].equals("TMIN")
                    ? new Temperature(year, Long.parseLong(s[3]), 0, 1, 0)
                    : new Temperature(year, 0, Long.parseLong(s[3]), 0, 1);
                context.write(sy, temp);
            }
        }
    }

    // each reduce call will process all Temperature values from all years for a station
    // in the order of:
    // ((stationID, 1880), Temp1)
    // ((stationID, 1881), Temp2)
    // ...
    public static class TempReducer
            extends Reducer<StationYear,Temperature,StationYear,Text> {
        public void reduce(StationYear key, Iterable<Temperature> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<Integer, Long> sumTMIN = new HashMap<>(),
                                   sumTMAX = new HashMap<>(),
                                   countTMIN = new HashMap<>(),
                                   countTMAX = new HashMap<>();
            for (Temperature temp : values) {
                Integer year = temp.getYear();
                sumTMIN.put(year, temp.getSumTMIN() + (sumTMIN.containsKey(year) ? sumTMIN.get(year) : 0));
                sumTMAX.put(year, temp.getSumTMAX() + (sumTMAX.containsKey(year) ? sumTMAX.get(year) : 0));
                countTMIN.put(year, temp.getCountTMIN() + (countTMIN.containsKey(year) ? countTMIN.get(year) : 0));
                countTMAX.put(year, temp.getCountTMAX() + (countTMAX.containsKey(year) ? countTMAX.get(year) : 0));
            }
            TreeMap<Integer, Long> sorted = new TreeMap<>(countTMIN);
            String s = "";
            for (Integer year : sorted.keySet()) {
                s += "(";
                s += Integer.toString(year) + ", ";
                s += Double.toString((double)sumTMIN.get(year) / countTMIN.get(year)) + ", ";
                s += Double.toString((double)sumTMAX.get(year) / countTMAX.get(year));
                s += ")";
            }
            Text result = new Text(s);
            context.write(key, result);
        }
    }

    // In order for each reduce call to process all-years data for each station
    public static class StationYearGroupingComparator extends WritableComparator {
        public StationYearGroupingComparator() {
            super(StationYear.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StationYear sya = (StationYear)a;
            StationYear syb = (StationYear)b;
            return sya.getStationID().compareTo(syb.getStationID());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Climate");
        job.setJarByClass(Climate.class);
        job.setMapperClass(TempMapper.class);
        job.setMapOutputKeyClass(StationYear.class);
        job.setMapOutputValueClass(Temperature.class);
        job.setGroupingComparatorClass(StationYearGroupingComparator.class);
        job.setReducerClass(TempReducer.class);
        job.setOutputKeyClass(StationYear.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
