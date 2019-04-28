package cn.itcast.pagecount2.urltopn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Properties;

public class UrlTopNJob {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();

        //加载资源文件
        Properties props = new Properties();
        props.load(UrlTopNJob.class.getClassLoader().getResourceAsStream("topn.properties"));

        conf.setInt("topN", Integer.parseInt(props.getProperty("TOPN")));
        Job job = Job.getInstance(conf);

        job.setJarByClass(UrlTopNJob.class);

        job.setMapperClass(UrlTopNMapper.class);
        job.setReducerClass(UrlTopNReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\url\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\url\\output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true)? 0: 1);

    }
}
