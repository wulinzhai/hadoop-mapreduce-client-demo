package cn.itcast.wcplus;

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

public class WordCountPlusStep1 {

    public static class WordCountPlusMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            String[] split = value.toString().split(" ");
            for (String s : split) {
                context.write(new Text(s + "-" + fileName), new IntWritable(1));
            }
        }
    }

    public static class WordCountPlusReducer1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountPlusStep1.class);

        job.setMapperClass(WordCountPlusMapper1.class);
        job.setReducerClass(WordCountPlusReducer1.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\wcplus\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\wcplus\\step1"));

        job.setNumReduceTasks(3);

        job.waitForCompletion(true);

    }

}
