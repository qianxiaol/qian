package join;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import util.JobUtil;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ForReduceJoin {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Info>{
        private Text okey=new Text();
        private Info ovalue=new Info();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            if(str.length<3){
                return;
            }
            //获取当前读的文件名
            FileSplit fileSplit=(FileSplit) context.getInputSplit();
            String fileName=fileSplit.getPath().getName();
            if(fileName.equals("order.txt")){//当前读到的是order
                okey.set(str[3]);
                ovalue.setOrderId(str[0]);
                ovalue.setOrderDate(str[1]);
                ovalue.setOrderCount(Integer.parseInt(str[2]));
                ovalue.setFlag(true);
            }else if(fileName.equals("product.txt")){//当前读到的是product
                okey.set(str[0]);
                ovalue.setProductId(str[0]);
                ovalue.setProductName(str[1]);
                ovalue.setProductPrice(Integer.parseInt(str[3]));
                ovalue.setFlag(false);
            }else{
                try {
                    throw new Exception("文件读取错误！！！");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,Info,Info,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
            Info product=new Info();
            List<Info> orderList=new ArrayList<>();
            for(Info info:values){
                try {
                    if (info.isFlag()) {
                        Info order = new Info();
                        BeanUtils.copyProperties(order, info);
                        orderList.add(order);
                        //System.out.println(orderList);
                    } else {
                        BeanUtils.copyProperties(product, info);
                    }
                }catch (IllegalAccessException e) {
                        e.printStackTrace();
                } catch (InvocationTargetException e) {
                        e.printStackTrace();
                }
            }
            //System.out.println(orderList);
            for(Info info:orderList){
                //System.out.println(info);
                info.setProductId(product.getProductId());
                info.setProductName(product.getProductName());
                info.setProductPrice(product.getProductPrice());
                context.write(info,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) {
        JobUtil.commitJob(ForReduceJoin.class,"D:\\测试文档\\forTestData\\jionData\\reduce","");
    }
}
