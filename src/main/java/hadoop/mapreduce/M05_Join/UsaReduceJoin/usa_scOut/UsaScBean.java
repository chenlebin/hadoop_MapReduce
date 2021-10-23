package hadoop.mapreduce.M05_Join.UsaReduceJoin.usa_scOut;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * @author: Suofen
 * description: TODO 实现自定义排序和序列化接口
 *                   WritableComparable<UsaScBean>
 * create time: TODO 2021/10/11 12:14
 *
  * @Param: null
 * @return
 */
public class UsaScBean implements WritableComparable<UsaScBean> {
    private String state;//州
    private String county;//县

    public UsaScBean() {
    }

    public UsaScBean(String state, String county) {
        this.state = state;
        this.county = county;
    }

    public void set(String state, String county) {
        this.state = state;
        this.county = county;
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

    @Override
    public String toString() {
        return state+"\t"+county;
    }

    /**
     * @author: Suofen
     * description: TODO 序列化方法没啥好说的
     * create time: TODO 2021/10/11 12:15
     *
      * @Param: null
     * @return
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(state);
        dataOutput.writeUTF(county);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.state = dataInput.readUTF();
        this.county = dataInput.readUTF();
    }

    @Override
    /**
     * @author: Suofen
     * description: TODO 自定义排序规则排序规则是先进行州的字典序比较，
     *                   当州相同的时候再进行县的比较也是按照字典序
     * create time: TODO 2021/10/11 18:51
     *
      * @Param: o
     * @return int
     */
    public int compareTo(UsaScBean o) {
        int i = state.compareTo(o.getState());
        int j = county.compareTo(o.getCounty());
        if (i > 0)
            return 1;
        else if (i < 0)
            return -1;
        else if (i == 0) {
            if (j > 0)
                return 1;
            else if (j < 0)
                return -1;
            else if (j == 0)
                return 0;
        }

        return 0;
    }
}
