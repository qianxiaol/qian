package forinput;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class SmallRecordReader extends RecordReader<Text,Text> {
    private Text key=new Text();
    private Text value=new Text();
    private boolean isfinish;
    private CombineFileSplit split;
    private TaskAttemptContext context;
    private FSDataInputStream inputStream;
    private Integer currentIndex;

    public SmallRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer currentIndex) {
        this.split = split;
        this.context = context;
        this.currentIndex = currentIndex;
    }

    public SmallRecordReader() {
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isfinish){
            Path path=split.getPath(currentIndex);
            String fileName=path.getName();
            key.set(fileName);
            FileSystem fs=path.getFileSystem(context.getConfiguration());
            inputStream=fs.open(path);
            byte[] content=new byte[(int)split.getLength(currentIndex)];
            inputStream.readFully(content);
            value.set(content);
            isfinish=true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isfinish?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}
