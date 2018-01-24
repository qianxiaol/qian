package wordCount;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordTimes implements WritableComparable<WordTimes>{
    private String word="";
    private int times;
    public WordTimes() {
    }

    public WordTimes(String word, int times) {
        this.times = times;
        this.word = word;

    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(word);
        dataOutput.writeInt(times);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.word=dataInput.readUTF();
        this.times=dataInput.readInt();
    }

    public int compareTo(WordTimes o) {
        return o.times-this.times;
}

    @Override
    public String toString() {
        return "WordTimes{" +
                "word='" + word + '\'' +
                ", times=" + times +
                '}';
    }
}
