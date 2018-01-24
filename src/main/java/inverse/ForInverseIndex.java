package inverse;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import util.JobUtil;

import java.io.IOException;

public class ForInverseIndex {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit=(FileSplit) context.getInputSplit();
            String fileName=fileSplit.getPath().getName();
            String str[]=value.toString().split(" ");
            for(String s:str){
                okey.set(s+" "+fileName);
                context.write(okey,ovalue);
            }
        }
    }
    public static class ForCombiner extends Reducer<Text,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for(Text text:values){
                count++;
            }
            String str[]=key.toString().split(" ");
            okey.set(str[0]);
            ovalue.set(str[1]+"-->"+count);
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**
             * 用于统计每个单词在每一个文章中出现的次数
             */
            /*StringBuffer sb=new StringBuffer();
            for(Text text:values){
                sb.append(text+",");
            }
            ovalue.set(sb.toString());
            context.write(key,ovalue);
            */
            /**
             * 用于判断某个单词在那一篇文章中出现的次数最多
             */
            int max=0;
            String string="";
            for(Text text:values){
                String str[]=text.toString().split("-->");
                if(Integer.parseInt(str[1])>max){
                    max=Integer.parseInt(str[1]);
                    string=str[0];
                }
            }
            okey.set(key);
            ovalue.set("出现最多的文章是:"+string+"，出现的次数是:"+max);
            //if(okey.toString().equals("java"))
            context.write(okey,ovalue);
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(ForInverseIndex.class,"D:\\测试文档\\forTestData\\inverseIndex\\data","",new ForCombiner());
    }
}
