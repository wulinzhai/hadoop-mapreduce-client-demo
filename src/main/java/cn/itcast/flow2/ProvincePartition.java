package cn.itcast.flow2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartition extends Partitioner<Text, FlowBean> {

    //相当于到内存数据库中取手机号归属地对应的省份
    private static HashMap<String, Integer> codeMap = new HashMap<>();
    static {
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
    }

    @Override
    public int getPartition(Text text, FlowBean flowBean, int i) {
        return codeMap.get(text.toString().substring(0, 3)) == null? 5: codeMap.get(text.toString().substring(0, 3));
    }
}
