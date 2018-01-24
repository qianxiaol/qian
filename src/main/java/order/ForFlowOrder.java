package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.JobUtil;

import java.io.IOException;

public class ForFlowOrder {
    public static class ForMapper extends Mapper<LongWritable,Text,FlowOrder,NullWritable>{
        private FlowOrder okey=new FlowOrder();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            okey.setPhone(str[0]);
            okey.setUpFlow(Integer.parseInt(str[1]));
            okey.setDownFlow(Integer.parseInt(str[2]));
            okey.setAllFlow(okey.getAllFlow());
            context.write(okey,NullWritable.get());
        }
    }

    public static void main(String[] args) {

        JobUtil.commitJob(ForFlowOrder.class,"D:\\测试文档\\allFlow","");
    }
}
