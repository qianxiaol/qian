package join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.JobUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class ForMapJoin {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Text okey=new Text();
        Map<String,String> cacheMap=new HashMap<>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String line=value.toString();
             String str[]=line.split("\t");
             String pn=cacheMap.get(str[2]);
             okey.set(line+"\t"+pn);
             context.write(okey,NullWritable.get());
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI cachefile=context.getCacheFiles()[0];
            BufferedReader br=new BufferedReader(new FileReader(new File(cachefile)));
            String temp;
            while((temp=br.readLine())!=null){
                String str[]=temp.split("\t");
                cacheMap.put(str[0],str[1]);
            }
        }
    }

    public static void main(String[] args) throws URISyntaxException {
        JobUtil.commitJob(ForMapJoin.class,"D:\\测试文档\\forTestData\\jionData\\map\\userinfo.txt","",new URI("file:///D://测试文档//forTestData//jionData//map//phoneinfo.txt"));
    }
}
