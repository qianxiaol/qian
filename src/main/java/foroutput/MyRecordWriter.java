package foroutput;

import com.mysql.jdbc.Driver;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import phoneFlow.Flow;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MyRecordWriter extends RecordWriter<Flow,NullWritable> {
    private static Connection conn;
    private static PreparedStatement ps;
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "root");
            ps=conn.prepareStatement("INSERT INTO tb_flow(phonenumber, upflow, downflow, allflow) VALUES (?,?,?,?)");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    @Override
    public void write(Flow flow, NullWritable nullWritable) throws IOException, InterruptedException {
        try {
            ps.setString(1,flow.getPhone());
            ps.setInt(2,flow.getUpFlow());
            ps.setInt(3,flow.getDownFlow());
            ps.setInt(4,flow.getAllFlow());
            ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        try {
            ps.close();
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }
}
