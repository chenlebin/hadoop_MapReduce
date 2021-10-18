package hadoop.mapreduce.M04_MySQL.Read.ReadBeans;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class usa_zongBean implements Writable, DBWritable {
    //��Ҫ��ȡ�������У�date county cases deaths
    private String date;
    private String county;
    private int cases;
    private int deaths;

    public usa_zongBean() {
    }

    public usa_zongBean(String date, String county, int cases, int deaths) {
        this.date = date;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
    }

    public void set(String date, String county, int cases, int deaths) {
        this.date = date;
        this.county = county;
        this.cases = cases;
        this.deaths = deaths;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public int getCases() {
        return cases;
    }

    public void setCases(int cases) {
        this.cases = cases;
    }

    public int getDeaths() {
        return deaths;
    }

    public void setDeaths(int deaths) {
        this.deaths = deaths;
    }

    @Override
    public String toString() {
        return date+","+county+","+cases+","+deaths;
    }


    /*---------------ʵ��Writable����������-----------------*/
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //��Ҫ��ȡ�������У�date county cases deaths
        dataOutput.writeUTF(date);
        dataOutput.writeUTF(county);
        dataOutput.writeInt(cases);
        dataOutput.writeInt(deaths);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //��Ҫ��ȡ�������У�date county cases deaths
        this.date = dataInput.readUTF();
        this.county = dataInput.readUTF();
        this.cases = dataInput.readInt();
        this.deaths = dataInput.readInt();
    }

    /*---------------ʵ��DBWritable����������-----------------*/
    @Override
    public void write(PreparedStatement statement) throws SQLException {
        //��Ҫ��ȡ�������У�date county cases deaths
        statement.setString(1,date);
        statement.setString(2,county);
        statement.setInt(3,cases);
        statement.setInt(4,deaths);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        //��Ҫ��ȡ�������У�date county cases deaths
        this.date = resultSet.getString(1);
        this.county = resultSet.getString(2);
        this.cases = resultSet.getInt(3);
        this.deaths = resultSet.getInt(4);
    }
}
