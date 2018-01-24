package counter;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import util.JobUtil;

import java.io.IOException;

import static counter.ForCounter.CountEnum.NUM;


public class ForCounter {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Counter sensitiveWord=context.getCounter("li","senWord");
            Counter notNumber=context.getCounter("li","notNumber");
            String line=value.toString();
            if(line.contains("the")){
                sensitiveWord.increment(1L);
            }
            Counter lineCount=context.getCounter(NUM);
            lineCount.increment(1L);
            try {
                Integer.parseInt(line);
            }catch (NumberFormatException e){
                notNumber.increment(1L);
            }
        }
    }
    public static enum CountEnum{
        NUM
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ForCounter.class,"D:\\测试文档\\forTestData\\javaApi","");
    }
}
