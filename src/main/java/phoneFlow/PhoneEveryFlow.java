package phoneFlow;

import foroutput.MyDBOutput;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class PhoneEveryFlow {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Flow>{
        private Text okey=new Text();
        private Flow ovalue=new Flow();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            okey.set(str[1]);
            int upFlow=Integer.parseInt(str[8]);
            int downFlow=Integer.parseInt(str[9]);
            ovalue.setPhone(str[1]);
            ovalue.setUpFlow(upFlow);
            ovalue.setDownFlow(downFlow);
            ovalue.setAllFlow(upFlow+downFlow);
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,Flow,Flow,NullWritable>{
        private Flow okey=new Flow();
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            int upsum=0;
            int downsum=0;
            for(Flow f:values){
                upsum+=f.getUpFlow();
                downsum+=f.getDownFlow();
            }
            okey.setPhone(key.toString());
            okey.setUpFlow(upsum);
            okey.setDownFlow(downsum);
            okey.setAllFlow(upsum+downsum);
            context.write(okey,NullWritable.get());
        }
    }

    public static void main(String[] args) {

        JobUtil.commitJob(PhoneEveryFlow.class,"D:\\测试文档\\phone","",new MyDBOutput());
    }
}
