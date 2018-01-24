package join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Info implements Writable{
    //order的信息
    private String orderId="";
    private String orderDate="";
    private int orderCount;
    //product的信息
    private String productId="";
    private String productName="";
    private int productPrice;
    /**
     * flag=true:order
     * flag=false:product
     */
    private boolean flag;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(String orderDate) {
        this.orderDate = orderDate;
    }

    public int getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(int orderCount) {
        this.orderCount = orderCount;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getProductPrice() {
        return productPrice;
    }

    public void setProductPrice(int productPrice) {
        this.productPrice = productPrice;
    }

    public boolean isFlag() {
        return flag;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(orderId);
        dataOutput.writeUTF(orderDate);
        dataOutput.writeInt(orderCount);
        dataOutput.writeUTF(productId);
        dataOutput.writeUTF(productName);
        dataOutput.writeInt(productPrice);
        dataOutput.writeBoolean(flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.orderId=dataInput.readUTF();
        this.orderDate=dataInput.readUTF();
        this.orderCount=dataInput.readInt();
        this.productId=dataInput.readUTF();
        this.productName=dataInput.readUTF();
        this.productPrice=dataInput.readInt();
        this.flag=dataInput.readBoolean();
    }

    @Override
    public String toString() {
        return "Info{" +
                "orderId='" + orderId + '\'' +
                ", orderDate='" + orderDate + '\'' +
                ", orderCount=" + orderCount +
                ", productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", productPrice=" + productPrice +
                ", flag=" + flag +
                '}';
    }
}
