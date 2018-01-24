package friend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class ForFindFans {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str1[]=value.toString().split(":");
            ovalue.set(str1[0]);
            String str2[]=str1[1].split(",");
            for(String s:str2){
                okey.set(s.toString());
                context.write(okey,ovalue);
            }
        }
    }
    public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();
            for(Text text:values){
                sb.append(text+",");
            }
            okey.set(key+":");
            ovalue.set(sb.toString());
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(ForFindFans.class,"D:\\测试文档\\forTestData\\friend","");
    }

}
