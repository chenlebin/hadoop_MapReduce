package hadoop.mapreduce.M04_MySQL.Beans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author: Suofen
 * description: TODO 实现序列化方法和DBWritable方法（涉及到数据库操作都要实现这个接口）
 * create time: TODO 2021/10/7 17:45
 *
 * @Param: null
 * @return
 */
public class UsaBean implements Writable, DBWritable {
    private int uid;//编号
    private String state;//州
    private String county;//县
    private long cases;//确诊人数
    private long deaths;//死亡人数

    public UsaBean() {
    }

    public UsaBean(int uid, String state, String county, long cases, long deaths) {
        this.uid = uid;
        this.state = state;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
    }

    public void set(int uid, String state, String county, long cases, long deaths) {
        this.uid = uid;
        this.state = state;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    public long getDeaths() {
        return deaths;
    }

    public void setDeaths(long deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return uid+","+state+","+county+","+cases+","+deaths;
    }
    /*---------------实现Writable的两个方法-----------------*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(uid);
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(county);
        dataOutput.writeLong(cases);
        dataOutput.writeLong(deaths);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid=dataInput.readInt();
        this.state=dataInput.readUTF();
        this.county=dataInput.readUTF();
        this.cases=dataInput.readLong();
        this.deaths=dataInput.readLong();
    }


    /*---------------实现DBWritable的两个方法-----------------*/
    @Override
    //在PreparedStatement中设置对象的字段  写数据字段
    public void write(PreparedStatement statement) throws SQLException {
        //set的两个参数，
        //第一个：顺序 编号
        //第二个：属性值
        statement.setInt(1,uid);
        statement.setString(2,state);
        statement.setString(3,county);
        statement.setLong(4,cases);
        statement.setLong(5,deaths);
    }

    @Override
    //从ResultSet读取查询的结果  赋值给对象属性  写数据操作
    public void readFields(ResultSet resultSet) throws SQLException {
        this.uid=resultSet.getInt(1);
        this.state=resultSet.getString(2);
        this.county=resultSet.getString(3);
        this.cases=resultSet.getLong(4);
        this.deaths=resultSet.getLong(5);

    }
}
