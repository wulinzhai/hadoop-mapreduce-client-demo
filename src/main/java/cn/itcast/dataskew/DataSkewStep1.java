package cn.itcast.dataskew;

import cn.itcast.friends.CommonFriendsStep1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

public class DataSkewStep1 {
    public static class DataSkewStep1Mapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        Random random = new Random();
        Text k = new Text();
        IntWritable v = new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int numReduceTasks = context.getNumReduceTasks();
            String[] words = value.toString().split(" ");
            for (String word : words) {
                k.set(word + "-" + random.nextInt(numReduceTasks));
                v.set(1);
                context.write(k, v);
            }
        }
    }

    public static class DataSkewStep1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        IntWritable v = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            v.set(count);
            context.write(key, v);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(DataSkewStep1.class);

        job.setMapperClass(DataSkewStep1Mapper.class);
        job.setReducerClass(DataSkewStep1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\wc\\input1"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\wc\\step1"));

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);

    }

}
