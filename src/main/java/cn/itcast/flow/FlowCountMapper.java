package cn.itcast.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, Flow> {
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] words = value.toString().split("\t");
        context.write(new Text(words[1]), new Flow(Integer.parseInt(words[words.length-3]), Integer.parseInt(words[words.length-2])));
    }
}
