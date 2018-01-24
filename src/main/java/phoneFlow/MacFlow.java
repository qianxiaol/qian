package phoneFlow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MacFlow implements Writable{
    private String Mac;
    private int flow;

    public String getMac() {
        return Mac;
    }

    public void setMac(String mac) {
        Mac = mac;
    }

    public int getFlow() {
        return flow;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(Mac);
        dataOutput.writeInt(flow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.Mac=dataInput.readUTF();
        this.flow=dataInput.readInt();
    }
}
