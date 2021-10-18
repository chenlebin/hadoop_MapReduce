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
 * description: TODO 此类用于验证Join操作是否需要MapReduce之中进行代码编写，
 *                  看看能不能直接通过修改SQL语句进行join连接
 *                  例如：
 *                  select * from usa INNER JOIN usa_datedr on usa.uid=usa_datedr.uid
 * create time: TODO 2021/10/9 18:36
 *
  * @Param: null
 * @return
 */

public class UsaZongBean implements Writable, DBWritable {
    private int uid;//编号
    private String state;//州
    private String county;//县
    private long cases;//确诊人数
    private long deaths;//死亡人数
    private String date;//时间
    private float dr;//死亡率

    public UsaZongBean() {
    }

    public UsaZongBean(int uid, String state, String county, long cases, long deaths, String date, float dr) {
        this.uid = uid;
        this.state = state;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
        this.date = date;
        this.dr = dr;
    }

    public void set(int uid, String state, String county, long cases, long deaths, String date, float dr) {
        this.uid = uid;
        this.state = state;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
        this.date = date;
        this.dr = dr;
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
        return uid+","+state+","+county+","+cases+","+deaths+","+date+","+dr;
    }


    /*---------------实现Writable的两个方法-----------------*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(uid);
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(county);
        dataOutput.writeLong(cases);
        dataOutput.writeLong(deaths);
        dataOutput.writeUTF(date);
        dataOutput.writeFloat(dr);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.uid=dataInput.readInt();
        this.state=dataInput.readUTF();
        this.county=dataInput.readUTF();
        this.cases=dataInput.readLong();
        this.deaths=dataInput.readLong();
        this.date=dataInput.readUTF();
        this.dr=dataInput.readFloat();
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
        statement.setString(6,date);
        statement.setFloat(7,dr);
    }

    @Override
    //从ResultSet读取查询的结果  赋值给对象属性  写数据操作
    public void readFields(ResultSet resultSet) throws SQLException {
        this.uid=resultSet.getInt(1);
        this.state=resultSet.getString(2);
        this.county=resultSet.getString(3);
        this.cases=resultSet.getLong(4);
        this.deaths=resultSet.getLong(5);
        this.date=resultSet.getString(6);
        this.dr=resultSet.getFloat(7);
    }
}
