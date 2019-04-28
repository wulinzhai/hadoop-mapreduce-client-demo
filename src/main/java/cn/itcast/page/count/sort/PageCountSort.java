package cn.itcast.page.count.sort;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PageCountSort implements WritableComparable<PageCountSort> {

    private String url;

    private Integer count;



    public String toString() {
        return this.url + "," + this.count;
    }

    public int compareTo(PageCountSort o) {
        if (o.count == this.count){
            return this.url.compareTo(o.url);
        }
        return o.count - this.count;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.url);
        dataOutput.writeInt(this.count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.url = dataInput.readUTF();
        this.count = dataInput.readInt();
    }
}
