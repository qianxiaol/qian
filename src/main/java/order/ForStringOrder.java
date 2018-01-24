package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class ForStringOrder {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Text okey=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            okey.set(value.toString());
            System.out.println(okey);
            context.write(okey,NullWritable.get());
        }
    }
    public static class ForReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            context.write(key,NullWritable.get());
        }
    }
    public static class ForStringComparator extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String str1=new String(b1,s1+1,l1-1);
            String str2=new String(b2,s2+1,l2-1);
            System.out.println(str1+","+str2);
            if(str1.length()==str2.length()){
                int a=Integer.parseInt(str1);
                int b=Integer.parseInt(str2);
                return b-a;
            }
            return str1.length()-str2.length();
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ForStringOrder.class,"D:\\测试文档\\排序","",new ForStringComparator());
    }
}
