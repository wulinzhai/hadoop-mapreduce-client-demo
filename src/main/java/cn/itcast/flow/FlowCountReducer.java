package cn.itcast.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, Flow, Text, Flow> {
    protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
        int uflow = 0;
        int dflow = 0;
        int totalflow = 0;
        for (Flow value : values) {
            uflow += value.getUpflow();
            dflow += value.getDflow();
        }
        totalflow = uflow + dflow;
        context.write(key, new Flow(uflow, dflow));
    }
}
