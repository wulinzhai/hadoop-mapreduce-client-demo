package cn.itcast.pagecount2.urltopn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

public class UrlTopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private TreeMap<PageBean, Object> pageMap = new TreeMap<>(new Comparator<PageBean>() {
        @Override
        public int compare(PageBean o1, PageBean o2) {
            return o2.getTimes() - o1.getTimes() == 0 ? o1.getUrl().compareTo(o2.getUrl()) : o2.getTimes() - o1.getTimes();
        }
    });

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += 1;
        }
        PageBean pageBean = new PageBean();
        pageBean.setUrl(key.toString());
        pageBean.setTimes(count);
        pageMap.put(pageBean, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int i = 0;
        int topN = context.getConfiguration().getInt("topN", 3);
        for (PageBean pageBean : pageMap.keySet()) {
            context.write(new Text(pageBean.getUrl()), new IntWritable(pageBean.getTimes()));
            i++;
            if (i==topN) return;
        }
    }
}
