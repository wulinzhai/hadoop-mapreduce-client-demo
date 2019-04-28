package cn.itcast.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class IndexStep2 {
    public static class IndexStep2Mapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            String[] split = fields[0].split("->");
            context.write(new Text(split[0]), new Text(split[1] + "->" + fields[1]));
        }
    }

    public static class IndexStep2Reducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value.toString() + " ");
            }
            context.write(key, new Text(sb.substring(0, sb.length()-1)));
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(IndexStep2.class);

        job.setMapperClass(IndexStep2Mapper.class);
        job.setReducerClass(IndexStep2Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\index\\step1-output"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\index\\step2-output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
