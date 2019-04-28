package cn.itcast.order.topn;

import cn.itcast.pagecount2.sort.UrlSortStep2;
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
import java.util.ArrayList;
import java.util.Collections;

public class OrderTopNJob {

    public static class OrderTopNJobMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
        OrderBean orderBean = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            orderBean.setOrderBean(fields[0], fields[1], fields[2], Float.parseFloat(fields[3]), Integer.parseInt(fields[4]));
            context.write(new Text(fields[0]), orderBean);
        }
    }

    public static class OrderTopNJobReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<OrderBean> orderBeanList = new ArrayList<>();
            for (OrderBean value : values) {
                OrderBean newBean = new OrderBean();
                newBean.setOrderBean(value.getOrderId(), value.getUserId(), value.getPdtName(), value.getPrice(), value.getNum());
                orderBeanList.add(newBean);
            }

            Collections.sort(orderBeanList);
            int i = 0;
            for (OrderBean orderBean : orderBeanList) {
                context.write(orderBean, NullWritable.get());
                if (++i == 3) return;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderTopNJob.class);

        job.setMapperClass(OrderTopNJobMapper.class);
        job.setReducerClass(OrderTopNJobReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\order\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\order\\output"));

        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
