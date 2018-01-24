package phoneFlow;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;
import java.net.URISyntaxException;


public class PhoneAllFlow {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text okey=new Text();
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            okey.set(str[1]);
            ovalue.set(Integer.parseInt(str[8])+Integer.parseInt(str[9]));
            context.write(okey,ovalue);
        }
    }
    public static class ForReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            context.write(key,new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
//        Job job= Job.getInstance();
//        job.setMapperClass(ForMapper.class);
//        job.setReducerClass(ForReduce.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        String path="D://output";
//        FileSystem fs=FileSystem.get(new URI("file://"+path),new Configuration());
//        if(fs.exists(new Path(path))){
//            fs.delete(new Path(path),true);
//        }
//        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\phone"));
//        FileOutputFormat.setOutputPath(job,new Path(path));
//        job.waitForCompletion(true);
        JobUtil.commitJob(PhoneAllFlow.class,"D:\\测试文档\\phone","");
    }
}
