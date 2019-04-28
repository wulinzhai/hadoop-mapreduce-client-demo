package cn.itcast.join;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Setter
@Getter
public class OrderUserBean implements WritableComparable<OrderUserBean>{
    private String orderId;
    private String userId;
    private String name;
    private Integer age;
    private String friend;
    private String tableName;


    public void setOrderUserBean(String orderId, String userId, String name, Integer age, String friend, String tableName) {
        this.orderId = orderId;
        this.userId = userId;
        this.name = name;
        this.age = age;
        this.friend = friend;
        this.tableName = tableName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.orderId);
        out.writeUTF(this.userId);
        out.writeUTF(this.name);
        out.writeInt(this.age);
        out.writeUTF(this.friend);
        out.writeUTF(this.tableName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.userId = in.readUTF();
        this.name = in.readUTF();
        this. age = in.readInt();
        this.friend = in.readUTF();
        this.tableName = in.readUTF();
    }


    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("");
        sb.append(orderId);
        sb.append(",").append(userId);
        sb.append(",").append(name);
        sb.append(",").append(age);
        sb.append(",").append(friend);
        return sb.toString();
    }

    @Override
    public int compareTo(OrderUserBean o) {
        return this.orderId.compareTo(o.orderId);
    }
}
