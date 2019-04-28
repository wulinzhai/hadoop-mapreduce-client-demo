package cn.itcast.topnplus2;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartition extends Partitioner<OrderBean2, NullWritable> {
    public int getPartition(OrderBean2 orderBean2, NullWritable nullWritable, int i) {
        return orderBean2.getOrderId().hashCode() % i ;
    }
}
