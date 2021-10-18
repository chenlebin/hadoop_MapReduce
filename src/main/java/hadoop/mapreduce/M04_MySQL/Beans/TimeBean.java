package hadoop.mapreduce.M04_MySQL.Beans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TimeBean implements Writable,DBWritable {
    private int uid;//编号
    private String date;//时间
    private float dr;//死亡率

    public TimeBean() {
    }

    public TimeBean(int uid, String date, float dr) {
        this.uid = uid;
        this.date = date;
        this.dr = dr;
    }

    public void set(int uid, String date, float dr) {
        this.uid = uid;
        this.date = date;
        this.dr = dr;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public float getDr() {
        return dr;
    }

    public void setDr(float dr) {
        this.dr = dr;
    }

    @Override
    public String toString() {
        return uid+","+date+","+dr;
    }

    /*---------------实现Writable的两个方法-----------------*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(uid);
        dataOutput.writeUTF(date);
        dataOutput.writeFloat(dr);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid=dataInput.readInt();
        this.date=dataInput.readUTF();
        this.dr=dataInput.readFloat();
    }

    /*---------------实现DBWritable的两个方法-----------------*/
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        statement.setInt(1,uid);
        statement.setString(2,date);
        statement.setFloat(3,dr);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.uid=resultSet.getInt(1);
        this.date=resultSet.getString(2);
        this.dr=resultSet.getFloat(3);
    }


}
