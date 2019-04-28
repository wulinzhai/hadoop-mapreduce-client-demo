package cn.itcast.page.count.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PageUrlTopnSubmint {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        //conf.setInt("topn", 3);

        /*Properties props = new Properties();
        props.load(PageUrlTopnSubmint.class.getClassLoader().getResourceAsStream("topn.properties"));
        String topn = props.getProperty("TOPN");
        conf.setInt("topn", Integer.parseInt(topn));*/

        conf.addResource("xx-oo.xml");

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageUrlTopnSubmint.class);

        job.setMapperClass(PageUrlTopnMapper.class);
        job.setReducerClass(PageUrlTopnReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(PageUrlCount.class);
        job.setOutputValueClass(Object.class);

        FileInputFormat.setInputPaths(job, new Path("G:\\mrdata\\page\\input"));
        FileOutputFormat.setOutputPath(job, new Path("G:\\mrdata\\page\\output"));

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);


    }
}
