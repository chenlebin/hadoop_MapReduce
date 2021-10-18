package hadoop.mapreduce.M02_Covid.fenzu;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author: Suofen
 * description: TODO 实心序列化排序接口WritableComparable
 * create time: TODO 2021/10/5 13:36
 *
 * @Param: null
 * @return
 */
public class CovidBean implements WritableComparable<CovidBean> {
    private String state;//州
    private String county;//县
    private long cases;//确诊病例数

    public CovidBean() {
    }

    public CovidBean(String state, String county, long cases) {
        this.state = state;
        this.county = county;
        this.cases = cases;
    }

    //自己封装set方法，用于对象属性赋值
    public void set(String state, String county, long cases) {
        this.state = state;
        this.county = county;
        this.cases = cases;
    }

    @Override
    public String toString() {
        return state+"\t"+county+"\t"+cases;
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

    @Override
    /**
     * @author: Suofen
     * description: TODO  重写排序规则：
     *                    首先根据州进行排序，字典序
     *                    当州相同的情况下进行下一步
     *                    对各县的确诊病例数进行倒序排序
     * create time: TODO 2021/10/5 13:36
     *
      * @Param: o
     * @return int
     */
    public int compareTo(CovidBean o) {
        //倒序排序
        int i = state.compareTo(o.getState());
        if(i > 0 )
            return 1;
        else if(i < 0)
            return -1;
        else
            return cases>o.getCases() ? -1:(cases<o.getCases() ? 1:0);
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO  序列化方法
     * create time: TODO 2021/10/5 13:36
     *
     * @Param: dataOutput
     * @return void
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(county);
        dataOutput.writeLong(cases);
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO  反序列化方法
     * create time: TODO 2021/10/5 13:36
     *
     * @Param: dataInput
     * @return void
     */
    public void readFields(DataInput dataInput) throws IOException {
        this.state=dataInput.readUTF();
        this.county= dataInput.readUTF();
        this.cases= dataInput.readLong();
    }

}
