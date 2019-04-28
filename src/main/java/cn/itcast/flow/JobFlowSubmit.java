package cn.itcast.flow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobFlowSubmit {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(JobFlowSubmit.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setPartitionerClass(ProvicePartition.class);
        //reduce数量要和partition一致
        job.setNumReduceTasks(6);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Flow.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\flow\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\flow\\provice-output"));

        job.waitForCompletion(true);

    }
}
