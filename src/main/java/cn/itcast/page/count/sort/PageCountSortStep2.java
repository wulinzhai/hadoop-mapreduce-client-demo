package cn.itcast.page.count.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class PageCountSortStep2 {

    public static class PageCountSortMapper2 extends Mapper<LongWritable, Text, PageCountSort, NullWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split("\t");
            context.write(new PageCountSort(words[0], Integer.parseInt(words[1])), NullWritable.get());
        }
    }

    public static class PageCountSortReducer2 extends Reducer<PageCountSort, NullWritable, PageCountSort, NullWritable>{
        protected void reduce(PageCountSort key, Iterable<Object> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageCountSortStep2.class);

        job.setMapperClass(PageCountSortMapper2.class);
        job.setReducerClass(PageCountSortReducer2.class);

        job.setMapOutputKeyClass(PageCountSort.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(PageCountSort.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\page\\step1"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\page\\step2"));

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);

    }


}
