package order;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class ForIntOrder {
    public static class ForMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable>{
        private IntWritable okey=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            okey.set(Integer.parseInt(value.toString()));
            System.out.println(okey+"======================");
            context.write(okey,NullWritable.get());
        }
    }

    public static class ForReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key+"========================");
            context.write(key,NullWritable.get());
        }
    }
    public static class ForIntComparator extends IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Job job= Job.getInstance();
//        job.setMapperClass(ForMapper.class);
//        job.setReducerClass(ForReducer.class);
//        job.setMapOutputKeyClass(IntWritable.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(NullWritable.class);
//        job.setSortComparatorClass(ForIntComparator.class);
//        String path="D://output";
//        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\排序"));
//        FileOutputFormat.setOutputPath(job,new Path(path));
//        ClearOutput.clear(path);
//        job.waitForCompletion(true);
         JobUtil.commitJob(ForIntOrder.class,"D:\\测试文档\\排序","",new ForIntComparator());
    }
}
