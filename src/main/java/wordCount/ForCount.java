package wordCount;

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
import util.ClearOutput;
import util.JobUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForCount {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            Pattern patten= Pattern.compile("\\w+");
            Matcher matcher=patten.matcher(line.trim());
            while(matcher.find()){
                String word=matcher.group();
                okey.set(word);
                context.write(okey,NullWritable.get());
            }
        }
    }
    public static class ForReducer extends Reducer<Text,NullWritable,Text,IntWritable>{
        private IntWritable ovalue=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;
            for(NullWritable i:values){
                sum++;
            }
            ovalue.set(sum);
            context.write(key,ovalue);
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        JobUtil.commitJob(ForCount.class,args[0],args[1]);
    }
}
