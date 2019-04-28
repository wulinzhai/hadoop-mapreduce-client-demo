package cn.itcast.order.grouptopN;

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

public class OrderGroupTopNJob {
    public static class OrderGroupTopNJobMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable>{
        OrderBean orderBean  = new OrderBean();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.setOrderBean(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
            context.write(orderBean, NullWritable.get());
        }
    }

    public static class OrderGroupTopNJobReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable>{
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (NullWritable value : values) {
                context.write(key, NullWritable.get());
                if (++i == 3) return;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderGroupTopNJob.class);

        job.setMapperClass(OrderGroupTopNJobMapper.class);
        job.setReducerClass(OrderGroupTopNJobReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\order\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\order\\output-2"));

        job.setPartitionerClass(OrderPartitioner.class);
        job.setGroupingComparatorClass(OrderGroupComprator.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
