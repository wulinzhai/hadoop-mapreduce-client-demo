package cn.itcast.friends2;

import cn.itcast.order.topn.OrderBean;
import cn.itcast.order.topn.OrderTopNJob;
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

public class CommonFriendsStep1 {

    public static class CommonFriendsStep1Mapper extends Mapper<LongWritable, Text, Text, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(":");
            String[] fields = split[1].split(",");
            for (String field : fields) {
                context.write(new Text(field), new Text(split[0]));
            }
        }
    }

    public static class CommonFriendsStep1Reducer extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text value : values) {
                sb.append(value).append(',');
            }
            context.write(key, new Text(sb.substring(0, sb.length()-1)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(CommonFriendsStep1.class);

        job.setMapperClass(CommonFriendsStep1Mapper.class);
        job.setReducerClass(CommonFriendsStep1Reducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\friends\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\friends\\step1"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
