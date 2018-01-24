package testMR;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCount {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str[]=value.toString().split(" ");
            for(String s:str){
                okey.set(s);
                context.write(okey,ovalue);
            }
        }
        public  static class ForReduce extends Reducer<Text,IntWritable,Text,NullWritable>{
            @Override
            protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum=0;
                for(IntWritable i:values){
                    sum++;
                }
                String result=key.toString()+","+sum;
                context.write(new Text(result),NullWritable.get());
            }
        }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Job job= Job.getInstance();
            job.setMapperClass(ForMapper.class);
            job.setReducerClass(ForReduce.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\b.txt"));
            FileOutputFormat.setOutputPath(job,new Path("D:\\测试文档\\wordCount1"));
            job.waitForCompletion(true);
        }
    }
}
