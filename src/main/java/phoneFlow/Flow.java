package phoneFlow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable {
    private String phone;
    private int upFlow;
    private int downFlow;
    private int allFlow;

    public Flow() {
    }

    public Flow(String phone, int upFlow, int downFlow, int allFlow) {
        this.phone = phone;
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.allFlow = allFlow;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getAllFlow() {
        return upFlow+downFlow;
    }

    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeUTF(phone);
         dataOutput.writeInt(upFlow);
         dataOutput.writeInt(downFlow);
         dataOutput.writeInt(allFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
         this.phone=dataInput.readUTF();
         this.upFlow=dataInput.readInt();
         this.downFlow=dataInput.readInt();
         this.allFlow=dataInput.readInt();
    }

    @Override
    public String toString() {
        return "Flow{" +
                "phone='" + phone + '\'' +
                ", upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", allFlow=" + allFlow +
                '}';
    }
}
