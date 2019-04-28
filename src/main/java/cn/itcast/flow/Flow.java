package cn.itcast.flow;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


@Getter
@Setter
@NoArgsConstructor
public class Flow implements Writable {


    private int upflow;
    private int dflow;
    private int totalflow;

    public Flow(int upflow, int dflow) {
        this.upflow = upflow;
        this.dflow = dflow;
        this.totalflow = upflow + dflow;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upflow);
        dataOutput.writeInt(dflow);
        dataOutput.writeInt(totalflow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readInt();
        this.dflow = dataInput.readInt();
        this.totalflow = dataInput.readInt();
    }

    public String toString() {
        return this.upflow + "," + this.dflow + "," + this.totalflow;
    }
}
