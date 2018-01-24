package forinput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.ClearOutput;
import util.JobUtil;

import java.io.IOException;

public class MultipleInputPath {
    public static class ForMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString()+","+value.toString());
            //System.out.println(key.toString());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /*Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\forTestData\\forClass"),
                                          new Path("D:\\测试文档\\forTestData\\forWordCount"));
        String path="D://output";
        ClearOutput.clear(path);
        FileOutputFormat.setOutputPath(job,new Path(path));
        job.waitForCompletion(true);*/
        Configuration configuration=new Configuration();
        //该配置设置后  达到分片大小即分片
        configuration.setLong("mapreduce.input.fileinputformat.split.maxsize",409600);
        JobUtil.commitJob(MultipleInputPath.class,"D:\\测试文档\\forTestData\\forTestData\\testkeyValue","",
                new SmallFileInputFormat());
    }
}
