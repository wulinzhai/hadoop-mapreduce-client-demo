package cn.itcast.topnplus;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TopnPlusMapper extends Mapper<LongWritable, Text, Text, OrderBean> {

    private OrderBean orderBean = new OrderBean();
    private Text text = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        orderBean.set(words[0], words[1], words[2], Float.parseFloat(words[3]), Integer.parseInt(words[4]));
        text.set(words[0]);
        context.write(text, orderBean);
    }
}
