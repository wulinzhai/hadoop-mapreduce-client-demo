package cn.itcast.pagecount2.urltopn;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
public class PageBean implements WritableComparable<PageBean> {   //这里WritableComparable<PageBean>如果写成Writable、Comparable<PageBean>会报错
    private String url;
    private Integer times;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.url);
        dataOutput.writeInt(this.times);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.url = dataInput.readUTF();
        this.times = dataInput.readInt();
    }

    @Override
    public int compareTo(PageBean o) {
        return o.times - this.times == 0 ? this.url.compareTo(o.url) : o.times - this.times;
    }
}
