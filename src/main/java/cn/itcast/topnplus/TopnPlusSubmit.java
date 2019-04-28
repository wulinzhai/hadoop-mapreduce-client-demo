package cn.itcast.topnplus;

import cn.itcast.wcplus.WordCountPlusStep1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopnPlusSubmit {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountPlusStep1.class);

        job.setMapperClass(TopnPlusMapper.class);
        job.setReducerClass(TopnPlusReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\order\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\order\\output"));

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);



    }
}
