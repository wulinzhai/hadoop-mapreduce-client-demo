package cn.itcast.join;

import cn.itcast.friends2.CommonFriendsStep2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class OrderJoin {
    public static class OrderJoinMapper extends Mapper<LongWritable, Text, Text, OrderUserBean> {

        OrderUserBean orderUserBean = new OrderUserBean();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String[] split = value.toString().split(",");
            if (fileName.startsWith("order")) {
                k.set(split[1]);
                orderUserBean.setOrderUserBean(split[0], split[1], "NULL", -1, "NULL", "order");
                context.write(k, orderUserBean);
            } else {
                k.set(split[0]);
                orderUserBean.setOrderUserBean("NULL", split[0], split[1], Integer.parseInt(split[2]), split[3], "user");
                context.write(k, orderUserBean);
            }

        }
    }

    public static class OrderJoinReducer extends Reducer<Text, OrderUserBean, OrderUserBean, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<OrderUserBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<OrderUserBean> orderUserBeans = new ArrayList<>();
            OrderUserBean userBean = new OrderUserBean();
            for (OrderUserBean value : values) {
                if ("order".equals(value.getTableName())){
                    OrderUserBean newBean = new OrderUserBean();
                    newBean.setOrderUserBean(value.getOrderId(), value.getUserId(), value.getName(), value.getAge(), value.getFriend(), value.getTableName());
                    orderUserBeans.add(newBean);
                }else {
                    userBean.setUserId(value.getUserId());
                    userBean.setAge(value.getAge());
                    userBean.setName(value.getName());
                    userBean.setFriend(value.getFriend());
                }
            }

            Collections.sort(orderUserBeans);
            for (OrderUserBean orderUserBean : orderUserBeans) {
                userBean.setOrderId(orderUserBean.getOrderId());
                context.write(userBean, NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderJoin.class);

        job.setMapperClass(OrderJoinMapper.class);
        job.setReducerClass(OrderJoinReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderUserBean.class);
        job.setOutputKeyClass(OrderUserBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\join\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\join\\output"));

        job.setNumReduceTasks(1);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
