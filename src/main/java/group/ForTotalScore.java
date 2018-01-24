package group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class ForTotalScore {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[]=value.toString().split(" ");
            okey.set(str[0]);
            ovalue.set(Integer.parseInt(str[1]));
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) {
        Configuration config=new Configuration();
        config.setBoolean(Job.MAP_OUTPUT_COMPRESS,true);
        config.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC,GzipCodec.class, CompressionCodec.class);
        JobUtil.commitJob(ForTotalScore.class,
                "D:\\测试文档\\forTestData\\forTestData\\gpinput",
                "",new GzipCodec(),config);
    }
}
