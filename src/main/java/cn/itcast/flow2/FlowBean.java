package cn.itcast.flow2;

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
public class FlowBean implements Writable{
    private int upflow;
    private int dflow;
    private int amountflow;

    public FlowBean(int upflow, int dflow) {
        this.upflow = upflow;
        this.dflow = dflow;
        this.amountflow = upflow + dflow;
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upflow);
        dataOutput.writeInt(dflow);
        dataOutput.writeInt(amountflow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readInt();
        this.dflow = dataInput.readInt();
        this.amountflow = dataInput.readInt();
    }
}
