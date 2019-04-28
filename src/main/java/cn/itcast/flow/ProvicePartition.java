package cn.itcast.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;
import java.util.Map;

public class ProvicePartition extends Partitioner<Text, Flow> {

    private static Map<String, Integer> codeMap = new HashMap<>();
    static {
        codeMap.put("135", 0);
        codeMap.put("136", 1);
        codeMap.put("137", 2);
        codeMap.put("138", 3);
        codeMap.put("139", 4);
    }

    public int getPartition(Text text, Flow flow, int i) {
        Integer code = codeMap.get(text.toString().substring(0, 3));
        return code == null? 5: code;
    }
}
