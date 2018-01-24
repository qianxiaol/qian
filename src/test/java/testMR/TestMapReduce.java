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

public class TestMapReduce {
    public static class TestMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private Text okey = new Text();
        private IntWritable ovalue = new IntWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            if (str.length == 3) {
                okey.set(str[0]);
                ovalue.set(Integer.parseInt(str[2]));
                context.write(okey, ovalue);
            }
        }
    }
    public static class TestReduce extends Reducer<Text,IntWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            int avg=0;
            int temp=0;
            for(IntWritable i:values){
                temp++;
                sum+=i.get();
            }
            avg=sum/temp;
            String result="班级编号："+key.toString()+", 总分："+sum+", 平均分："+avg;
            context.write(new Text(result),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\a.txt"));
        FileOutputFormat.setOutputPath(job,new Path("D:\\测试文档\\c"));
        job.waitForCompletion(true);
    }
}
