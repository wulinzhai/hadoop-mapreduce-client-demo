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

public class CommonFriendsStep2 {

    public static class CommonFriendsMapper2 extends Mapper<LongWritable, Text, Text, Text>{
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            context.write(new Text(split[0]), new Text(split[1]));
        }
    }

    public static class CommonFriendsReducer2 extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder builder = new StringBuilder();
            for (Text value : values) {
                builder.append(value.toString()).append(",");
            }
            int i = builder.lastIndexOf(",");
            String substring = builder.substring(0, i);
            context.write(key, new Text(substring));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStep2.class);

        job.setMapperClass(CommonFriendsMapper2.class);
        job.setReducerClass(CommonFriendsReducer2.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\friends\\step1"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\friends\\step2"));

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }

}
