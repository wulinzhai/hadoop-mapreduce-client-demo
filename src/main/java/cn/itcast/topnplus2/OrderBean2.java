package cn.itcast.topnplus2;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter@Setter
@NoArgsConstructor
public class OrderBean2 implements WritableComparable<OrderBean2>{

    private String orderId;
    private String userId;
    private String productName;
    private Float price;
    private Integer number;

    private Float totalAmount;

    public void set(String orderId, String userId, String productName, Float price, Integer number) {
        this.orderId = orderId;
        this.userId = userId;
        this.productName = productName;
        this.price = price;
        this.number = number;
        this.totalAmount = price * number;
    }

    public String toString() {
        return orderId + "," + userId + "," + productName + "," + price + "," + number + "," + totalAmount;
    }

    public int compareTo(OrderBean2 o) {
        return this.getOrderId().compareTo(o.getOrderId()) == 0? Float.compare(o.getTotalAmount(), this.getTotalAmount()): this.getOrderId().compareTo(o.getOrderId());
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(userId);
        dataOutput.writeUTF(productName);
        dataOutput.writeFloat(price);
        dataOutput.writeInt(number);
        dataOutput.writeFloat(totalAmount);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readUTF();
        this.userId = dataInput.readUTF();
        this.productName = dataInput.readUTF();
        this.price = dataInput.readFloat();
        this.number = dataInput.readInt();
        this.totalAmount = dataInput.readFloat();
    }
}
