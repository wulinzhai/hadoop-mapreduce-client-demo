package cn.itcast.friends2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;

public class CommonFriendsStep2 {

    public static class CommonFriendsStep2Mapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            String[] fields = split[1].split(",");
            Arrays.sort(fields);
            for (int i = 0; i < fields.length-1; i++){
                for (int j = i+1; j < fields.length; j++){
                    context.write(new Text(fields[i] + "-" + fields[j]), new Text(split[0]));
                }
            }
        }
    }

    public static class CommonFriendsStep2Reducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString()).append(',');
            }
            context.write(key, new Text(sb.substring(0, sb.length()-1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStep2.class);

        job.setMapperClass(CommonFriendsStep2Mapper.class);
        job.setReducerClass(CommonFriendsStep2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\friends\\step1"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\friends\\step2"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
