package wordCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import util.ClearOutput;
import util.JobUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ForCountTopN {
    public static class ForMapper extends Mapper<LongWritable,Text,WordTimes,NullWritable>{
        private List<WordTimes> list=new ArrayList<WordTimes>();
        {
            for(int i=0;i<4;i++){
                list.add(new WordTimes());
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            WordTimes wordTimes=new WordTimes(str[0],Integer.parseInt(str[1]));
            list.add(wordTimes);
            Collections.sort(list);
            list.remove(3);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<3;i++)
                 context.write(list.get(i),NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        Job job= Job.getInstance();
//        job.setMapperClass(ForMapper.class);
//        job.setMapOutputKeyClass(WordTimes.class);
//        job.setMapOutputValueClass(NullWritable.class);
//        String path="D://output";
//        ClearOutput.clear(path);
//        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\wordtimes"));
//        FileOutputFormat.setOutputPath(job,new Path(path));
//
//        job.waitForCompletion(true);
        JobUtil.commitJob(ForCountTopN.class,args[0],args[1]);
    }
}
