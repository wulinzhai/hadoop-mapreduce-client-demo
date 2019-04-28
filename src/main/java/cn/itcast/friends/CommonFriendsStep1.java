package cn.itcast.friends;

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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CommonFriendsStep1 {

    public static class CommonFriendsMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String user = split[0];
            String[] friends = split[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend), new Text(user));
            }
        }
    }

    public static class CommonFriendsReducer1 extends Reducer<Text, Text, Text, Text>{
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text value : values) {
                list.add(value.toString());
            }
            Collections.sort(list);
            for (int i = 0; i < list.size() - 1; i++){
                for (int j = i + 1; j < list.size(); j++){
                    context.write(new Text(list.get(i) + "-" + list.get(j)), key);
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStep1.class);

        job.setMapperClass(CommonFriendsMapper1.class);
        job.setReducerClass(CommonFriendsReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\friends\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\friends\\step1"));

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }



}
