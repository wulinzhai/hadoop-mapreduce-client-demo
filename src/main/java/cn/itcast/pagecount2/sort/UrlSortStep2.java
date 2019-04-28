package cn.itcast.pagecount2.sort;

import cn.itcast.pagecount2.urltopn.PageBean;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class UrlSortStep2 {

    public static class UrlSortStep2Mapper extends Mapper<LongWritable, Text, PageBean, NullWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            PageBean pageBean = new PageBean();
            pageBean.setUrl(fields[0]);
            pageBean.setTimes(Integer.parseInt(fields[1]));
            context.write(pageBean, NullWritable.get());
        }
    }

    public static class UrlSortStep2Reducer extends Reducer<PageBean, NullWritable, Text, IntWritable>{
        @Override
        protected void reduce(PageBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getUrl()), new IntWritable(key.getTimes()));
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(UrlSortStep2.class);

        job.setMapperClass(UrlSortStep2Mapper.class);
        job.setReducerClass(UrlSortStep2Reducer.class);

        job.setMapOutputKeyClass(PageBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\url\\step1-output"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\url\\step2-output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)? 0: 1);

    }



}
