package cn.itcast.topnplus2;

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

public class TopnPlus {

    public static class TopnOrderMapper extends Mapper<LongWritable, Text, OrderBean2, NullWritable> {
        private OrderBean2 orderBean2 = new OrderBean2();
        NullWritable v = NullWritable.get();

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean2.set(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
            context.write(orderBean2, v);
        }
    }

    public static class TopnOrderReducer extends Reducer<OrderBean2, NullWritable, OrderBean2, NullWritable> {
        protected void reduce(OrderBean2 key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (NullWritable value : values) {
                context.write(key, value);
                if (++i == 3) {
                    return;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(TopnPlus.class);

        job.setPartitionerClass(OrderIdPartition.class);
        job.setGroupingComparatorClass(OrderIdGroupingComparator.class);

        job.setMapperClass(TopnOrderMapper.class);
        job.setReducerClass(TopnOrderReducer.class);

        job.setMapOutputKeyClass(OrderBean2.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean2.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\order\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\order\\output-2"));

        job.setNumReduceTasks(2);

        job.waitForCompletion(true);

    }


}
