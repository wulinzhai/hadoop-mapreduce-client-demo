package cn.itcast.order.grouptopN;

import cn.itcast.order.topn.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartitioner extends Partitioner<OrderBean, NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int i) {
        return (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % i;
    }
}
