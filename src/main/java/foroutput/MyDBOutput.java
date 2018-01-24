package foroutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import phoneFlow.Flow;

import java.io.IOException;

public class MyDBOutput extends FileOutputFormat <Flow,NullWritable>{
    @Override
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MyRecordWriter();
    }
}
