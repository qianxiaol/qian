package combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.JobUtil;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class ForCombinerAvg {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,AvgSum>{
        private Text okey=new Text();
        private AvgSum ovalue=new AvgSum();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split(" ");
            okey.set(str[0]);
            ovalue.setSum(Integer.parseInt(str[1]));
            ovalue.setCount(1);
            context.write(okey,ovalue);
        }
    }
    public static class ForCombiner extends Reducer<Text,AvgSum,Text,AvgSum>{
        private AvgSum ovalue=new AvgSum();
        @Override
        protected void reduce(Text key, Iterable<AvgSum> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int count=0;
            for(AvgSum as:values){
                sum+=as.getSum();
                count+=as.getCount();
            }
            ovalue.setSum(sum);
            ovalue.setCount(count);
            context.write(key,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,AvgSum,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<AvgSum> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int count=0;
            for(AvgSum as:values){
                sum+=as.getSum();
                count+=as.getCount();
            }
            context.write(key,new DoubleWritable(sum/count));
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
//        Job job= Job.getInstance();
//        job.setMapperClass(ForMapper.class);
//        job.setReducerClass(ForReducer.class);
//        job.setCombinerClass(ForCombiner.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(AvgSum.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(DoubleWritable.class);
//        String path="D://output";
//        FileSystem fs=FileSystem.get(new URI("file://"+path),new Configuration());
//        if(fs.exists(new Path(path))){
//            fs.delete(new Path(path));
//        }
//        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\forTestData\\forCombiner"));
//        FileOutputFormat.setOutputPath(job,new Path(path));
//        job.waitForCompletion(true);
        JobUtil.commitJob(ForCombinerAvg.class,"D:\\测试文档\\forTestData\\forCombiner","");
    }
}
