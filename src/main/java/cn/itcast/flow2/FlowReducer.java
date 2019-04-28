package cn.itcast.flow2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upflow = 0;
        int dflow = 0;
        for (FlowBean value : values) {
            upflow += value.getUpflow();
            dflow += value.getDflow();
        }
        int amountflow= upflow + dflow;
        context.write(key, new Text(upflow + "\t" + dflow + "\t" + amountflow));
    }
}
