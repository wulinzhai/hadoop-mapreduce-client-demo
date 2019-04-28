package cn.itcast.order.topn;


import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class OrderBean implements WritableComparable<OrderBean> {

    private String orderId;
    private String userId;
    private String pdtName;
    private Float price;
    private Integer num;
    private Float amountPrice;

    public void setOrderBean(String orderId, String userId, String pdtName, Float price, Integer num) {
        this.orderId = orderId;
        this.userId = userId;
        this.pdtName = pdtName;
        this.price = price;
        this.num = num;
        this.amountPrice = price * num;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.orderId);
        dataOutput.writeUTF(this.userId);
        dataOutput.writeUTF(this.pdtName);
        dataOutput.writeFloat(this.price);
        dataOutput.writeInt(this.num);
        dataOutput.writeFloat(this.amountPrice);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.pdtName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.num = dataInput.readInt();
        this.amountPrice = dataInput.readFloat();
    }

    @Override
    public int compareTo(OrderBean o) {
        return this.orderId.compareTo(o.orderId) == 0? Float.compare(o.amountPrice, this.amountPrice): this.orderId.compareTo(o.orderId);
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("");
        sb.append(orderId);
        sb.append(",").append(userId);
        sb.append(",").append(pdtName);
        sb.append(",").append(price);
        sb.append(",").append(num);
        sb.append(",").append(amountPrice);
        return sb.toString();
    }
}
