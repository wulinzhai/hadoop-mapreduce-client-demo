package cn.itcast.page.count.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PageUrlTopnReducer extends Reducer<Text, IntWritable, PageUrlCount, Object> {
    private Map<PageUrlCount, Object> map = new TreeMap<>();
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        map.put(new PageUrlCount(key.toString(), count), null);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int i = 0;
        for (PageUrlCount pageUrlCount : map.keySet()) {
            context.write(pageUrlCount, null);
            i++;
            if (i == Integer.parseInt(conf.get("topn", "5"))){
                break;
            }
        }
    }
}
