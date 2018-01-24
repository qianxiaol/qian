package order;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;

public class ForFlowOrderMR {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,FlowOrder>{
        private Text okey=new Text();
        private FlowOrder ovalue=new FlowOrder();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            okey.set(str[1]);
            ovalue.setPhone(str[1]);
            int upFlow=Integer.parseInt(str[8]);
            int downFlow=Integer.parseInt(str[9]);
            ovalue.setUpFlow(upFlow);
            ovalue.setDownFlow(downFlow);
            ovalue.setAllFlow(ovalue.getAllFlow());
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,FlowOrder,FlowOrder,NullWritable>{
        private FlowOrder okey=new FlowOrder();
        @Override
        protected void reduce(Text key, Iterable<FlowOrder> values, Context context) throws IOException, InterruptedException {
            int upsum=0;
            int downsum=0;
            for(FlowOrder fo:values){
                upsum+=fo.getUpFlow();
                downsum+=fo.getDownFlow();
            }
            okey.setPhone(key.toString());
            okey.setUpFlow(upsum);
            okey.setDownFlow(downsum);
            okey.setAllFlow(okey.getAllFlow());
            context.write(okey,NullWritable.get());
        }
    }
    public static class ForPartitioner extends Partitioner<Text,FlowOrder>{

        @Override
        public int getPartition(Text text, FlowOrder flowOrder, int i) {
            String numPhone=text.toString();
            String num=numPhone.substring(0,3);
            if(num.equals("135")||(num.equals("136"))||(num.equals("137"))){
                return 1;
            }
            return 0;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//         Job job= Job.getInstance();
//         job.setMapperClass(ForMapper.class);
//         job.setReducerClass(ForReducer.class);
//         job.setMapOutputKeyClass(Text.class);
//         job.setMapOutputValueClass(FlowOrder.class);
//         job.setOutputKeyClass(FlowOrder.class);
//         job.setOutputValueClass(NullWritable.class);
//         job.setPartitionerClass(ForPartitioner.class);
//         job.setNumReduceTasks(2);
//         String path="D://output";
//        FileInputFormat.setInputPaths(job,new Path("D:\\测试文档\\phone"));
//        FileOutputFormat.setOutputPath(job,new Path(path));
//        ClearOutput.clear(path);
//        job.waitForCompletion(true);
        JobUtil.commitJob(ForFlowOrderMR.class,"D:\\测试文档\\phone","",new ForPartitioner(),2);
    }
}



