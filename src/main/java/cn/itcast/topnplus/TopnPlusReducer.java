package cn.itcast.topnplus;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopnPlusReducer extends Reducer<Text, OrderBean, OrderBean, NullWritable> {

    protected void reduce(Text key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
        List<OrderBean> orderBeans = new ArrayList();
        for (OrderBean value : values) {
            OrderBean orderBean = new OrderBean();
            orderBean.set(value.getOrderId(), value.getUserId(), value.getProductName(), value.getPrice(), value.getNumber());
            orderBeans.add(orderBean);
        }
        Collections.sort(orderBeans);
        int i = 0;
        for (OrderBean order : orderBeans) {
            context.write(order, NullWritable.get());
            i++;
            if (i == 3) {
                break;
            }
        }
    }
}
