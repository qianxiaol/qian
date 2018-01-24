package phoneFlow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import util.JobUtil;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ForMacFlow {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,MacFlow>{
        private Text okey=new Text();
        private MacFlow ovalue=new MacFlow();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[]=line.split("\t");
            okey.set(str[1]);
            ovalue.setMac(str[2]);
            ovalue.setFlow(Integer.parseInt(str[8])+Integer.parseInt(str[9]));
            context.write(okey,ovalue);
        }
    }
    public static class ForReducer extends Reducer<Text,MacFlow,Text,Text>{

        @Override
        protected void reduce(Text key, Iterable<MacFlow> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map=new HashMap();
            for(MacFlow mf:values){
                String mac=mf.getMac();
                int flow=mf.getFlow();
                if(map.containsKey(mac)){
                    map.put(mac,map.get(mac)+flow);
                }
                map.put(mac,flow);
            }
            StringBuffer sb=new StringBuffer();
            for(Map.Entry entry:map.entrySet()){
                sb.append(entry.getKey()+":"+entry.getValue()+"\t");
            }
            context.write(key,new Text(sb.toString()));
        }
    }

    public static void main(String[] args) {

        JobUtil.commitJob(ForMacFlow.class,"D:\\测试文档\\phone","");
    }
}
