package group;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MyClassComparator extends WritableComparator{
    public MyClassComparator() {
        super(Text.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        return a.toString().substring(0,1).compareTo(b.toString().substring(0,1));
    }
}
